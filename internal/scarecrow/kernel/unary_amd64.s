//go:build amd64 && !purego

#include "textflag.h"

DATA ·absMask+0(SB)/8, $0x7fffffffffffffff
GLOBL ·absMask(SB), RODATA|NOPTR, $8

TEXT ·scaleAVX(SB), NOSPLIT, $0-32
	MOVQ dst_base+0(FP), AX
	MOVQ dst_len+8(FP), CX
	VBROADCASTSD factor+24(FP), Y0
scale_loop:
	CMPQ CX, $4
	JL scale_tail
	VMOVUPD (AX), Y1
	// Match Go operand order, including NaN payload selection.
	VMULPD Y0, Y1, Y1
	VMOVUPD Y1, (AX)
	ADDQ $32, AX
	SUBQ $4, CX
	JMP scale_loop
scale_tail:
	TESTQ CX, CX
	JE scale_done
	VMOVSD (AX), X1
	VMULSD X0, X1, X1
	VMOVSD X1, (AX)
	ADDQ $8, AX
	DECQ CX
	JMP scale_tail
scale_done:
	VZEROUPPER
	RET

TEXT ·absAVX(SB), NOSPLIT, $0-24
	MOVQ dst_base+0(FP), AX
	MOVQ dst_len+8(FP), CX
	VBROADCASTSD ·absMask(SB), Y0
abs_loop:
	CMPQ CX, $4
	JL abs_tail
	VANDPD (AX), Y0, Y1
	VMOVUPD Y1, (AX)
	ADDQ $32, AX
	SUBQ $4, CX
	JMP abs_loop
abs_tail:
	TESTQ CX, CX
	JE abs_done
	VMOVSD (AX), X1
	VANDPD X1, X0, X1
	VMOVSD X1, (AX)
	ADDQ $8, AX
	DECQ CX
	JMP abs_tail
abs_done:
	VZEROUPPER
	RET

TEXT ·absDiffAVX(SB), NOSPLIT, $0-56
	MOVQ dst_base+0(FP), AX
	MOVQ dst_len+8(FP), CX
	MOVQ src_base+24(FP), DX
	VBROADCASTSD center+48(FP), Y0
	VBROADCASTSD ·absMask(SB), Y2
diff_loop:
	CMPQ CX, $4
	JL diff_tail
	VMOVUPD (DX), Y1
	// Keep subtraction order identical to the scalar Go loop.
	VSUBPD Y0, Y1, Y1
	VANDPD Y2, Y1, Y1
	VMOVUPD Y1, (AX)
	ADDQ $32, AX
	ADDQ $32, DX
	SUBQ $4, CX
	JMP diff_loop
diff_tail:
	TESTQ CX, CX
	JE diff_done
	VMOVSD (DX), X1
	VSUBSD X0, X1, X1
	VANDPD X2, X1, X1
	VMOVSD X1, (AX)
	ADDQ $8, AX
	ADDQ $8, DX
	DECQ CX
	JMP diff_tail
diff_done:
	VZEROUPPER
	RET
