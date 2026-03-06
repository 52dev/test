package constant

//
//import "go-wind-admin/api/gen/go/k8s/service/v1"
//
//// GPUDatabase 全局显卡数据库
//// Key 是系列的前缀标识，用于模糊匹配
//var GPUDatabase = map[string]servicev1.GPUSeries{
//	"A100": {
//		SeriesName:  "NVIDIA A100",
//		SeriesKey:   "A100",
//		DefaultVRAM: 40, // 如果匹配不到具体型号，默认认为是 40G
//		Models: map[string]*servicev1.GPUSpec{
//			"SXM4-40GB": {ModelName: "NVIDIA A100-SXM4-40GB", VRAM: 40},
//			"SXM4-80GB": {ModelName: "NVIDIA A100-SXM4-80GB", VRAM: 80},
//			"PCIE-40GB": {ModelName: "NVIDIA A100-PCIE-40GB", VRAM: 40},
//		},
//	},
//	"V100": {
//		SeriesName:  "NVIDIA V100",
//		SeriesKey:   "V100",
//		DefaultVRAM: 16,
//		Models: map[string]*servicev1.GPUSpec{
//			"SXM2-16GB": {ModelName: "Tesla V100-SXM2-16GB", VRAM: 16},
//			"SXM2-32GB": {ModelName: "Tesla V100-SXM2-32GB", VRAM: 32},
//			"PCIE-32GB": {ModelName: "Tesla V100-PCIE-32GB", VRAM: 32},
//		},
//	},
//	"H20": {
//		SeriesName:  "NVIDIA H20",
//		SeriesKey:   "H20",
//		DefaultVRAM: 16,
//		Models: map[string]*servicev1.GPUSpec{
//			"16GB": {ModelName: "NVIDIA H20-16GB", VRAM: 16},
//			"32GB": {ModelName: "NVIDIA H20-32GB", VRAM: 32},
//		},
//	},
//}
//
//// 镜像列表
//var DefaultImages = []servicev1.Image{
//	{
//		Name:        "PyTorch 26.01",
//		URL:         "nvcr.io/nvidia/pytorch:26.01-py3",
//		Description: "PyTorch 26.01 with Ubuntu 24.04 and CUDA 13.1.1.006 and PyTorch 2.10.0a0+a36e1d39eb and TensorRT 10.14.1.48. more https://docs.nvidia.com/deeplearning/frameworks/pytorch-release-notes/rel-26-01.html#rel-26-01",
//		MinCpu:      1,
//		MinMem:      2,
//		MinGpu:      1,
//	},
//	{
//		Name:        "TensorFlow 25.02",
//		URL:         "nvcr.io/nvidia/tensorflow:25.02-tf2-py3-igpu",
//		Description: "TensorFlow 25.02 with Ubuntu 24.04 and CUDA 12.8.0.38 and TensorFlow 2.17.0 and TensorRT 10.8.0.43. more https://docs.nvidia.com/deeplearning/frameworks/tensorflow-release-notes/rel-25-02.html#rel-25-02",
//		MinCpu:      1,
//		MinMem:      2,
//		MinGpu:      1,
//	},
//}
