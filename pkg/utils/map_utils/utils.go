package map_utils

func MergeMaps(map1, map2 map[string]string) map[string]string {
	for key, value := range map2 {
		map1[key] = value
	}

	return map1
}
