package slice

import (
	"fmt"
	"strconv"
)

// MergeInPlace 原地合并（不创建新切片，覆盖原切片）
func MergeInPlace(slice1, slice2 []uint32) []uint32 {
	// 计算需要的总容量
	totalLen := len(slice1) + len(slice2)

	// 如果slice1容量不足，创建一个新的足够大的切片
	if cap(slice1) < totalLen {
		newSlice := make([]uint32, len(slice1), totalLen)
		copy(newSlice, slice1)
		slice1 = newSlice
	}

	// 扩展slice1的长度，并复制slice2的元素
	slice1 = slice1[:totalLen]
	copy(slice1[len(slice1)-len(slice2):], slice2)

	return slice1
}

// MergeAndDeduplicateOrdered 有序去重合并（不允许重复元素，保持原顺序）
func MergeAndDeduplicateOrdered(slice1, slice2 []uint32) []uint32 {
	seen := make(map[uint32]struct{})
	result := make([]uint32, 0, len(slice1)+len(slice2))

	// 先添加slice1的元素（保持顺序）
	for _, v := range slice1 {
		if _, exists := seen[v]; !exists {
			seen[v] = struct{}{}
			result = append(result, v)
		}
	}

	// 再添加slice2的元素（跳过已存在的）
	for _, v := range slice2 {
		if _, exists := seen[v]; !exists {
			seen[v] = struct{}{}
			result = append(result, v)
		}
	}

	return result
}

// MergeAndDeduplicate 去重合并（不允许重复元素，无序）
func MergeAndDeduplicate(slice1, slice2 []uint32) []uint32 {
	set := make(map[uint32]struct{})
	for _, v := range slice1 {
		set[v] = struct{}{}
	}
	for _, v := range slice2 {
		set[v] = struct{}{}
	}

	result := make([]uint32, 0, len(set))
	for v := range set {
		result = append(result, v)
	}
	return result
}

// Unique 对切片进行去重，保持元素原有顺序。
// 泛型类型 T 需要是 comparable，以便用作 map 的键。
func Unique[T comparable](s []T) []T {
	if len(s) == 0 {
		return s
	}
	seen := make(map[T]struct{}, len(s))
	out := make([]T, 0, len(s))
	for _, v := range s {
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			out = append(out, v)
		}
	}
	return out
}

// Intersect 计算两个切片的交集，返回包含在两个切片中都出现的唯一元素的新切片。
// 泛型类型 T 需要是 comparable，以便用作 map 的键。
func Intersect[T comparable](a, b []T) []T {
	if len(a) == 0 || len(b) == 0 {
		return []T{}
	}
	m := make(map[T]struct{}, len(b))
	for _, v := range b {
		m[v] = struct{}{}
	}
	out := make([]T, 0, len(a))
	seen := make(map[T]struct{}, len(a))
	for _, v := range a {
		if _, ok := m[v]; ok {
			if _, s := seen[v]; !s {
				out = append(out, v)
				seen[v] = struct{}{}
			}
		}
	}
	return out
}

// NumberSliceToStrings 将数值型切片转换为 string 切片。
// 支持：int,int8,int16,int32,int64,uint,uint8,uint16,uint32,uint64,float32,float64。
// 对未知类型回退使用 fmt.Sprint。
func NumberSliceToStrings[T ~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64](s []T) []string {
	if len(s) == 0 {
		return nil
	}
	out := make([]string, 0, len(s))
	for _, v := range s {
		switch x := any(v).(type) {
		case int:
			out = append(out, strconv.FormatInt(int64(x), 10))
		case int8:
			out = append(out, strconv.FormatInt(int64(x), 10))
		case int16:
			out = append(out, strconv.FormatInt(int64(x), 10))
		case int32:
			out = append(out, strconv.FormatInt(int64(x), 10))
		case int64:
			out = append(out, strconv.FormatInt(x, 10))
		case uint:
			out = append(out, strconv.FormatUint(uint64(x), 10))
		case uint8:
			out = append(out, strconv.FormatUint(uint64(x), 10))
		case uint16:
			out = append(out, strconv.FormatUint(uint64(x), 10))
		case uint32:
			out = append(out, strconv.FormatUint(uint64(x), 10))
		case uint64:
			out = append(out, strconv.FormatUint(x, 10))
		case float32:
			out = append(out, strconv.FormatFloat(float64(x), 'f', -1, 32))
		case float64:
			out = append(out, strconv.FormatFloat(x, 'f', -1, 64))
		default:
			out = append(out, fmt.Sprint(v))
		}
	}
	return out
}
