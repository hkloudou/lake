package lake

func updateResult(result *map[string]any, file *ossFileProperty) {
	if result == nil {
		tmp := make(map[string]any)
		result = &tmp
	}
	current := *result
	for i, field := range file.Field {
		if _, ok := current[field]; !ok {
			current[field] = make(map[string]any)
		}
		if i == len(file.Field)-1 { // Last element
			if file.Merge == MergeTypeOver {
				if file.Value == nil {
					delete(current, field)
				} else {
					current[field] = file.Value
				}
			} else if file.Merge == MergeTypeUpsert {
				if _, ok := current[field]; !ok {
					current[field] = make(map[string]any)
				}
				for k, v := range file.Value.(map[string]any) {
					if v == nil {
						delete(current[field].(map[string]any), k)
					} else {
						current[field].(map[string]any)[k] = v
					}
				}
			}
		} else {
			current = current[field].(map[string]any)
		}
	}
	if len(file.Field) == 0 { // Root directory operation
		if file.Merge == MergeTypeOver {
			if file.Value == nil {
				*result = nil
			} else {
				*result = file.Value.(map[string]any)
			}
		} else if file.Merge == MergeTypeUpsert {
			// if _, ok := (*result).(map[string]any); !ok {
			for k, v := range file.Value.(map[string]any) {
				if v == nil {
					delete((*result), k)
				} else {
					(*result)[k] = v
				}
			}
			// }
		}
	}
}
