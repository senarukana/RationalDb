package proto

type QueryExecutor interface {
	ExecuteFetch(query string, maxrows int, wantfields bool) (qr *proto.QueryResult, err error)
	ExecuteStreamFetch(query string) (err error)
	Fields() (fields []proto.Field)
	FetchNext() (row []sqltypes.Value, err error)
	CloseResult()
}
