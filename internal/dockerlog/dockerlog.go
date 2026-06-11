// Package dockerlog provides Docker container log parser.
package dockerlog

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/errors"
	mobycontainer "github.com/moby/moby/api/types/container"
	mobyclient "github.com/moby/moby/client"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"golang.org/x/sync/errgroup"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

// Querier implements LogQL querier.
type Querier struct {
	client mobyclient.APIClient
}

// NewQuerier creates new Querier.
func NewQuerier(c mobyclient.APIClient) (*Querier, error) {
	return &Querier{
		client: c,
	}, nil
}

// Capabilities returns Querier capabilities.
// NOTE: engine would call once and then save value.
//
// Capabilities should not change over time.
func (q *Querier) Capabilities() (caps logqlengine.QuerierCapabilities) {
	caps.Label.Add(logql.OpEq, logql.OpNotEq, logql.OpRe, logql.OpNotRe)
	return caps
}

var _ logqlengine.Querier = (*Querier)(nil)

// Query creates new [InputNode].
func (q *Querier) Query(ctx context.Context, labels []logql.LabelMatcher) (logqlengine.PipelineNode, error) {
	return &InputNode{
		Lables: labels,
		q:      q,
	}, nil
}

// InputNode is an input for LogQL pipeline using Docker API.
type InputNode struct {
	Lables []logql.LabelMatcher

	q *Querier
}

var _ logqlengine.PipelineNode = (*InputNode)(nil)

// Traverse implements [logqlengine.Node].
func (n *InputNode) Traverse(cb logqlengine.NodeVisitor) error {
	return cb(n)
}

// EvalPipeline implements [logqlengine.PipelineNode].
func (n *InputNode) EvalPipeline(ctx context.Context, params logqlengine.EvalParams) (_ logqlengine.EntryIterator, rerr error) {
	containers, err := n.q.fetchContainers(ctx, n.Lables)
	if err != nil {
		return nil, errors.Wrap(err, "fetch containers")
	}
	switch len(containers) {
	case 0:
		return iterators.Empty[logqlengine.Entry](), nil
	case 1:
		return n.q.openLog(ctx, containers[0], params.Start, params.End)
	default:
		iters := make([]logqlengine.EntryIterator, len(containers))
		defer func() {
			// Close all iterators in case of error.
			if rerr != nil {
				for _, iter := range iters {
					if iter == nil {
						continue
					}
					_ = iter.Close()
				}
			}
		}()

		// FIXME(tdakkota): errgroup cancels group context
		// when Wait is done.
		//
		// It cancels request to Docker daemon, so we use query context to avoid this.
		// As a result, openLog context would not be canceled in case of error.
		var grp errgroup.Group
		for idx, ctr := range containers {
			grp.Go(func() error {
				iter, err := n.q.openLog(ctx, ctr, params.Start, params.End)
				if err != nil {
					return errors.Wrapf(err, "open container %q log", ctr.ID)
				}
				iters[idx] = iter
				return nil
			})
		}
		if err := grp.Wait(); err != nil {
			return nil, err
		}
		return newMergeIter(iters), nil
	}
}

func (q *Querier) openLog(ctx context.Context, ctr container, start, end time.Time) (logqlengine.EntryIterator, error) {
	var since, until string
	if t := start; !t.IsZero() {
		since = strconv.FormatInt(t.Unix(), 10)
	}
	if t := end; !t.IsZero() {
		until = strconv.FormatInt(t.Unix(), 10)
	}

	rc, err := q.client.ContainerLogs(ctx, ctr.ID, mobyclient.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Since:      since,
		Until:      until,
		Timestamps: true,
		Tail:       "all",
	})
	if err != nil {
		return nil, errors.Wrap(err, "query logs")
	}
	return ParseLog(rc, ctr.labels.AsResource()), nil
}

func (q *Querier) fetchContainers(ctx context.Context, labels []logql.LabelMatcher) (r []container, _ error) {
	result, err := q.client.ContainerList(ctx, mobyclient.ContainerListOptions{
		All: true,
		// TODO(tdakkota): convert select params to label matchers.
	})
	if err != nil {
		return nil, errors.Wrap(err, "query container list")
	}

	for _, ctr := range result.Items {
		set := getLabels(ctr)
		if set.Match(labels) {
			r = append(r, container{
				ID:     ctr.ID,
				labels: set,
			})
		}
	}
	return r, nil
}

type container struct {
	ID     string
	labels containerLabels
}

type containerLabels struct {
	labels map[string]string
}

func getLabels(ctr mobycontainer.Summary) containerLabels {
	var name string
	if len(ctr.Names) > 0 {
		name = strings.TrimPrefix(ctr.Names[0], "/")
	}
	labels := map[string]string{
		"container":          name,
		"container_id":       ctr.ID,
		"container_name":     name,
		"container_image":    ctr.Image,
		"container_image_id": ctr.ImageID,
		"container_command":  ctr.Command,
		"container_created":  strconv.FormatInt(ctr.Created, 10),
		"container_state":    string(ctr.State),
		"container_status":   ctr.Status,
	}
	for label, value := range ctr.Labels {
		labels[otelstorage.KeyToLabel(label)] = value
	}
	return containerLabels{
		labels: labels,
	}
}

func (c containerLabels) Match(matchers []logql.LabelMatcher) bool {
	for _, matcher := range matchers {
		value, ok := c.labels[string(matcher.Label)]
		if !ok {
			return false
		}
		if !match(matcher, value) {
			return false
		}
	}
	return true
}

func (c containerLabels) AsResource() otelstorage.Attrs {
	attrs := otelstorage.Attrs(pcommon.NewMap())
	for key, value := range c.labels {
		attrs.AsMap().PutStr(key, value)
	}
	return attrs
}

func match(m logql.LabelMatcher, s string) bool {
	switch m.Op {
	case logql.OpEq:
		return s == m.Value
	case logql.OpNotEq:
		return s != m.Value
	case logql.OpRe:
		return m.Re.MatchString(s)
	case logql.OpNotRe:
		return !m.Re.MatchString(s)
	default:
		return false
	}
}
