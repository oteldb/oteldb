//go:build amd64 && !purego

package kernel

import "golang.org/x/sys/cpu"

var simdEnabled = cpu.X86.HasAVX

//go:noescape
func scaleAVX(dst []float64, factor float64)

//go:noescape
func absAVX(dst []float64)

//go:noescape
func absDiffAVX(dst, src []float64, center float64)
