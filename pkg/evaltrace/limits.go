package evaltrace

const (
	DefaultMaxTraceBytes  = 2 * 1024 * 1024
	DefaultMaxRecords     = 2000
	DefaultMaxRecordBytes = 16 * 1024
	DefaultMaxCorrections = 8

	HardMaxTraceBytes  = 16 * 1024 * 1024
	HardMaxRecords     = 10000
	HardMaxRecordBytes = 64 * 1024
	HardMaxCorrections = 64
)

func DefaultLimits() AppliedLimits {
	return AppliedLimits{
		MaxTraceBytes:  DefaultMaxTraceBytes,
		MaxRecords:     DefaultMaxRecords,
		MaxRecordBytes: DefaultMaxRecordBytes,
		MaxCorrections: DefaultMaxCorrections,
	}
}

func NormalizeLimits(in AppliedLimits) AppliedLimits {
	defaults := DefaultLimits()
	if in.MaxTraceBytes <= 0 {
		in.MaxTraceBytes = defaults.MaxTraceBytes
	}
	if in.MaxRecords <= 0 {
		in.MaxRecords = defaults.MaxRecords
	}
	if in.MaxRecordBytes <= 0 {
		in.MaxRecordBytes = defaults.MaxRecordBytes
	}
	if in.MaxCorrections <= 0 {
		in.MaxCorrections = defaults.MaxCorrections
	}
	in.MaxTraceBytes = min(in.MaxTraceBytes, HardMaxTraceBytes)
	in.MaxRecords = min(in.MaxRecords, HardMaxRecords)
	in.MaxRecordBytes = min(in.MaxRecordBytes, HardMaxRecordBytes)
	in.MaxCorrections = min(in.MaxCorrections, HardMaxCorrections)
	return in
}
