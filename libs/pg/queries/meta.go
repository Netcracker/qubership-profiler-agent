package queries

const (
	// Create queries

	InsertPod = `INSERT INTO %s (pod_id, namespace, service_name, pod_name, active_since, last_restart, last_active, tags) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

	InsertPodRestart = `INSERT INTO %s (pod_id, namespace, service_name, pod_name, restart_time, active_since, last_active) 
		VALUES ($1, $2, $3, $4, $5, $6, $7)`

	InsertParam = `INSERT INTO %s (pod_id, pod_name, restart_time, param_name, param_index, param_list, param_order, signature) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

	InsertDictionary = `INSERT INTO %s (pod_id, pod_name, restart_time, position, tag) 
		VALUES ($1, $2, $3, $4, $5)`

	// Read queries

	GetUniqueNamespaces = `SELECT DISTINCT(namespace) FROM %s`

	GetUniquePodsForNamespaceActiveAfter = `SELECT DISTINCT pod_id, namespace, service_name, pod_name, active_since, last_restart, last_active, tags FROM %s WHERE namespace=$1 AND last_active > $2 ORDER BY service_name ASC, pod_name ASC`

	GetUniquePodsForNamespaceActiveBefore = `SELECT DISTINCT pod_id, namespace, service_name, pod_name, active_since, last_restart, last_active, tags FROM %s WHERE namespace=$1 AND last_active < $2 ORDER BY service_name ASC, pod_name ASC`

	GetTagByPosition = `SELECT tag FROM %s WHERE position = $1`

	GetPodRestarts = `SELECT pod_id, namespace, service_name, pod_name, restart_time, active_since, last_active FROM %s WHERE namespace=$1 AND service_name=$2 AND pod_name=$3`

	// Update queries

	// Delete queries

	RemoveByPodId = `DELETE FROM %s
		WHERE pod_id=$1`
)
