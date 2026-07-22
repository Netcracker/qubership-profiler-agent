//go:build darwin

package emulator

// fionread is sys/ioctl.h's FIONREAD (_IOR('f', 127, int)); x/sys/unix does
// not export it for darwin.
const fionread = 0x4004667f
