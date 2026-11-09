//go:build !amd64 || purego

package kernel

const simdEnabled = false

func scaleAVX(dst []float64, factor float64)        { scaleF64(dst, factor) }
func absAVX(dst []float64)                          { absF64(dst) }
func absDiffAVX(dst, src []float64, center float64) { absDiffF64(dst, src, center) }
