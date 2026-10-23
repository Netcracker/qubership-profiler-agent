package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCallClass(t *testing.T) {
	for method, want := range map[string]string{
		"void com.example.OrderService.place(long) (OrderService.java) [orders.jar]": "com.example.OrderService",
		"com.example.OrderService.place":                                             "com.example.OrderService",
		"java.lang.String com.example.Outer$Inner.run()":                             "com.example.Outer$Inner",
		"Main.main(String[])":                                                        "Main",
		"#42":                                                                        "",
		"main()":                                                                     "",
		"":                                                                           "",
	} {
		assert.Equal(t, want, CallClass(method), method)
	}
}

func TestCallEndpoint(t *testing.T) {
	for _, tc := range []struct {
		url, method string
		want        string
	}{
		{"http://orders:8080/orders/123?expand=items", "GET", "GET /orders/{id}"},
		{"https://h/api/v1/users/550e8400-e29b-41d4-a716-446655440000/carts/9", "POST", "POST /api/v1/users/{id}/carts/{id}"},
		{"/files/0123456789abcdef0123/meta#top", "GET", "GET /files/{id}/meta"},
		{"/files/0123456789abcde/v2", "GET", "GET /files/0123456789abcde/v2"}, // 15 hex digits stay
		{"http://host", "GET", "GET /"},
		{"http://host?q=/a/1", "GET", "GET /"},
		{"/orders/", "", "/orders/"},
		{"/orders/12a", "DELETE", "DELETE /orders/12a"},
	} {
		params := map[string][]string{ParamWebURL: {tc.url}}
		if tc.method != "" {
			params[ParamWebMethod] = []string{tc.method}
		}
		got, ok := CallEndpoint(params)
		assert.True(t, ok, tc.url)
		assert.Equal(t, tc.want, got, tc.url)
	}
	_, ok := CallEndpoint(map[string][]string{ParamWebMethod: {"GET"}})
	assert.False(t, ok, "a call without web.url has no endpoint")
	_, ok = CallEndpoint(map[string][]string{ParamWebURL: {}})
	assert.False(t, ok, "a web.url with no value counts as absent")
}

func TestMatcherDerivedFields(t *testing.T) {
	r := matchRow()
	r.Params[ParamWebURL] = []string{"http://billing/charges/77"}
	r.Params[ParamWebMethod] = []string{"POST"}
	noClass := matchRow()
	noClass.Method = "#12"
	noURL := matchRow()
	for _, tc := range []struct {
		r    CallRow
		text string
		want bool
	}{
		{r, "class=com.example.Billing", true},
		{r, "class!=com.example.Billing", false},
		{r, "class=~Bill", true},
		{r, "class!~^org\\.", true},
		{r, "class in com.example.Orders,com.example.Billing", true},
		{r, "class not in com.example.Billing", false},
		{noClass, "class=~.", false},
		{noClass, "class!=com.example.Billing", true}, // a method that names no class leaves the call without one
		{r, "endpoint=POST /charges/{id}", true},
		{r, "endpoint=~^POST ", true},
		{r, "endpoint!~charges", false},
		{r, "endpoint in GET /x,POST /charges/{id}", true},
		{r, "endpoint not in POST /charges/{id}", false},
		{r, "endpoint=Any", true},
		{r, "endpoint=None", false},
		{noURL, "endpoint=None", true},
		{noURL, "endpoint!=Any", true},
		{noURL, "endpoint!=GET /", true},
		{noURL, "endpoint=~.", false},
		// An absent derived field has no values, not an empty one: a regex
		// that matches the empty string does not select the call, and its
		// negation does (02 §2.3.3).
		{noClass, "class=~.*", false},
		{noClass, "class=~^$", false},
		{noClass, "class!~.*", true},
		{noClass, "class!~^$", true},
		{noClass, "class in com.example.Billing", false},
		{noClass, "class not in com.example.Billing", true},
		{noURL, "endpoint=~.*", false},
		{noURL, "endpoint=~^$", false},
		{noURL, "endpoint!~.*", true},
		{noURL, "endpoint!~^$", true},
		{noURL, "endpoint=Any", false},
		{noURL, "endpoint!=None", false},
		{noURL, "endpoint not in GET /", true},
	} {
		assert.Equal(t, tc.want, matches(t, tc.r, tc.text), tc.text)
	}
}

func TestFieldValues(t *testing.T) {
	r := matchRow()
	r.ErrorFlag = true
	for field, want := range map[string][]string{
		"service":         {"billing"},
		"error":           {"true"},
		"retention_class": {RetentionNormalClean},
		"class":           {"com.example.Billing"},
		"user.tenant":     {"acme", "globex"},
		"trace_id":        {"4bf92f3577b34da6a3ce929d0e0e4736"},
	} {
		got, ok := FieldValues(field, &r)
		assert.True(t, ok, field)
		assert.Equal(t, want, got, field)
	}
	for _, field := range []string{"duration", "endpoint", "missing", "empty"} {
		got, ok := FieldValues(field, &r)
		assert.False(t, ok, field)
		assert.Empty(t, got, field)
	}
	r.Method = "#42"
	got, ok := FieldValues("class", &r)
	assert.False(t, ok, "a method that names no class")
	assert.Empty(t, got, "an absent class has no values, not an empty one")
}
