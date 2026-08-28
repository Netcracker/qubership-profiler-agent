// Production fault drivers: client-go for pod-delete/scale/readiness,
// a minimal REST client for toxiproxy. Tests use the interface fakes.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// kubeClient resolves the config the way the checker does: kubeconfig
// (KUBECONFIG or the default home path) first, in-cluster as the fallback.
type kubeClient struct {
	client kubernetes.Interface
}

func newKubeClient() (*kubeClient, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		cfg, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("no kubeconfig and no in-cluster config: %w", err)
		}
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &kubeClient{client: client}, nil
}

func (k *kubeClient) DeletePod(ctx context.Context, namespace, pod string) error {
	grace := int64(0) // the crash shape, not a drain
	return k.client.CoreV1().Pods(namespace).Delete(ctx, pod, metav1.DeleteOptions{GracePeriodSeconds: &grace})
}

func (k *kubeClient) PodReady(ctx context.Context, namespace, pod string) (bool, error) {
	p, err := k.client.CoreV1().Pods(namespace).Get(ctx, pod, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue, nil
		}
	}
	return false, nil
}

func (k *kubeClient) GetScale(ctx context.Context, namespace, kind, name string) (int32, error) {
	s, err := k.scales(namespace, kind).get(ctx, name)
	if err != nil {
		return 0, err
	}
	return s.Spec.Replicas, nil
}

func (k *kubeClient) SetScale(ctx context.Context, namespace, kind, name string, replicas int32) error {
	sc := k.scales(namespace, kind)
	s, err := sc.get(ctx, name)
	if err != nil {
		return err
	}
	s.Spec.Replicas = replicas
	return sc.update(ctx, s)
}

// scaler folds the statefulset/deployment split behind one pair of calls.
type scaler struct {
	get    func(ctx context.Context, name string) (*autoscalingv1.Scale, error)
	update func(ctx context.Context, s *autoscalingv1.Scale) error
}

func (k *kubeClient) scales(namespace, kind string) scaler {
	if kind == "statefulset" {
		c := k.client.AppsV1().StatefulSets(namespace)
		return scaler{
			get: func(ctx context.Context, name string) (*autoscalingv1.Scale, error) {
				return c.GetScale(ctx, name, metav1.GetOptions{})
			},
			update: func(ctx context.Context, s *autoscalingv1.Scale) error {
				_, err := c.UpdateScale(ctx, s.Name, s, metav1.UpdateOptions{})
				return err
			},
		}
	}
	c := k.client.AppsV1().Deployments(namespace)
	return scaler{
		get: func(ctx context.Context, name string) (*autoscalingv1.Scale, error) {
			return c.GetScale(ctx, name, metav1.GetOptions{})
		},
		update: func(ctx context.Context, s *autoscalingv1.Scale) error {
			_, err := c.UpdateScale(ctx, s.Name, s, metav1.UpdateOptions{})
			return err
		},
	}
}

// toxiClient is the minimal toxiproxy REST slice the fault layer needs.
type toxiClient struct {
	base string
	http *http.Client
}

func newToxiClient(base string) *toxiClient {
	return &toxiClient{base: base, http: &http.Client{Timeout: 15 * time.Second}}
}

func (t *toxiClient) CreateToxic(ctx context.Context, proxy, name, typ string, attrs map[string]any) error {
	body, err := json.Marshal(map[string]any{
		"name": name, "type": typ, "stream": "downstream", "attributes": attrs,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/proxies/%s/toxics", t.base, proxy), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return t.do(req, http.StatusOK)
}

func (t *toxiClient) DeleteToxic(ctx context.Context, proxy, name string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		fmt.Sprintf("%s/proxies/%s/toxics/%s", t.base, proxy, name), nil)
	if err != nil {
		return err
	}
	err = t.do(req, http.StatusNoContent)
	if err != nil && isToxiNotFound(err) {
		return nil // deleting an absent toxic is a no-op: the revert is idempotent
	}
	return err
}

func (t *toxiClient) ListToxicNames(ctx context.Context, proxy string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/proxies/%s/toxics", t.base, proxy), nil)
	if err != nil {
		return nil, err
	}
	resp, err := t.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, toxiError(resp)
	}
	var toxics []struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&toxics); err != nil {
		return nil, err
	}
	names := make([]string, len(toxics))
	for i, tx := range toxics {
		names[i] = tx.Name
	}
	return names, nil
}

func (t *toxiClient) do(req *http.Request, want int) error {
	resp, err := t.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != want && resp.StatusCode != http.StatusOK {
		return toxiError(resp)
	}
	return nil
}

type toxiHTTPError struct {
	status int
	body   string
}

func (e *toxiHTTPError) Error() string {
	return fmt.Sprintf("toxiproxy: HTTP %d: %s", e.status, e.body)
}

func isToxiNotFound(err error) bool {
	he, ok := err.(*toxiHTTPError)
	return ok && he.status == http.StatusNotFound
}

func toxiError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
	return &toxiHTTPError{status: resp.StatusCode, body: string(body)}
}
