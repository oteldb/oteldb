import { useGetHealth } from "../api/admin";
import { Card, Pill, Spinner, ErrorBox } from "../components/ui";

export function Health() {
  const { data, isLoading, error } = useGetHealth({ query: { refetchInterval: 5_000 } });

  return (
    <>
      <div className="section-title">Component health</div>
      <Card title="Services" sub={data ? <Pill status={data.status} /> : undefined}>
        {isLoading ? (
          <Spinner />
        ) : error ? (
          <ErrorBox error={error} />
        ) : data ? (
          <div>
            {data.components.map((c) => (
              <div key={c.name}>
                <div className="health-row">
                  <span>
                    <span className="name">{c.name}</span>
                    {c.addr ? <span className="addr">{c.addr}</span> : null}
                  </span>
                  <Pill status={c.status} />
                </div>
                {c.error ? <div className="health-err">{c.error}</div> : null}
              </div>
            ))}
          </div>
        ) : null}
      </Card>
    </>
  );
}
