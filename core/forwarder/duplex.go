package forwarder

import (
	"context"
	"fmt"

	"github.com/gotd/td/tg"
	"golang.org/x/sync/errgroup"

	"github.com/iyear/tdl/core/util/tutil"
)

// DuplexOptions bounds group lifetimes and each transfer direction independently.
// One client and namespace own every RPC; no copied authorization is needed.
type DuplexOptions struct{ Groups, Downloads, Uploads int }

type TransferPool struct{ downloads, uploads chan struct{} }

// NewTransferPool creates direction budgets shared by independent requests.
func NewTransferPool(downloads, uploads int) *TransferPool {
	return &TransferPool{make(chan struct{}, downloads), make(chan struct{}, uploads)}
}

func transferStage(ctx context.Context, slots chan struct{}, run func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-slots }()
	return run(ctx)
}

func (p *TransferPool) transfer(ctx context.Context, download, upload func(context.Context) error, ready func(context.Context) error) error {
	workers, ctx := errgroup.WithContext(ctx)
	workers.Go(func() error { return transferStage(ctx, p.downloads, download) })
	workers.Go(func() error {
		// Waiting for the first bytes must not occupy another file's upload slot.
		if ready != nil {
			if err := ready(ctx); err != nil {
				return err
			}
		}
		return transferStage(ctx, p.uploads, upload)
	})
	return workers.Wait()
}

func prepareAlbum(ctx context.Context, messages []*tg.Message, prepare func(context.Context, *tg.Message) (tg.InputMediaClass, error)) ([]tg.InputMediaClass, error) {
	prepared := make([]tg.InputMediaClass, len(messages))
	workers, ctx := errgroup.WithContext(ctx)
	for index, message := range messages {
		workers.Go(func() error {
			media, err := prepare(ctx, message)
			if err != nil {
				return fmt.Errorf("prepare album member %d: %w", message.ID, err)
			}
			prepared[index] = media // Each worker owns a distinct index, preserving source order.
			return nil
		})
	}
	if err := workers.Wait(); err != nil {
		return nil, err
	}
	return prepared, nil
}

func (f *Forwarder) forwardDuplex(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	opts := f.opts.Duplex
	if opts.Groups < 1 || opts.Downloads < 1 || opts.Uploads < 1 {
		return fmt.Errorf("duplex concurrency must be positive")
	}
	workers, ctx := errgroup.WithContext(ctx)
	workers.SetLimit(opts.Groups)
	for f.opts.Iter.Next(ctx) {
		elem := f.opts.Iter.Value()
		if elem.Mode() != ModeClone {
			cancel()
			return waitForwarders(workers, fmt.Errorf("duplex requires clone mode"))
		}
		key := f.tuple(elem.From(), elem.Msg())
		if _, ok := f.sent[key]; ok {
			continue
		}
		var grouped []*tg.Message
		if _, ok := elem.Msg().GetGroupedID(); ok && elem.AsGrouped() {
			var err error
			grouped, err = tutil.GetGroupedMessages(ctx, f.opts.Pool.Default(ctx), elem.From().InputPeer(), elem.Msg())
			if err != nil {
				cancel()
				return waitForwarders(workers, err)
			}
			if len(grouped) == 0 {
				cancel()
				return waitForwarders(workers, fmt.Errorf("source album is empty"))
			}
			for _, message := range grouped {
				f.sent[f.tuple(elem.From(), message)] = struct{}{}
			}
		}
		// Only this coordinator writes deduplication state. Workers own separate
		// forwarders, sharing only direction budgets and the safe global RNG.
		f.sent[key] = struct{}{}
		workers.Go(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			worker := New(f.opts)
			worker.pipeline = f.pipeline
			return worker.forwardMessage(ctx, elem, grouped...)
		})
	}
	if f.opts.Iter.Err() != nil {
		cancel()
	}
	return waitForwarders(workers, f.opts.Iter.Err())
}

func waitForwarders(workers *errgroup.Group, inputError error) error {
	if err := workers.Wait(); err != nil {
		return err
	}
	return inputError
}

func (f *Forwarder) entities(entities []tg.MessageEntityClass) []tg.MessageEntityClass {
	if !f.opts.RemoveSpoilers {
		return entities
	}
	result := make([]tg.MessageEntityClass, 0, len(entities))
	for _, entity := range entities {
		if _, spoiler := entity.(*tg.MessageEntitySpoiler); !spoiler {
			result = append(result, entity)
		}
	}
	return result
}

func clearSpoiler(media tg.InputMediaClass) {
	switch media := media.(type) {
	case *tg.InputMediaPhoto:
		media.SetSpoiler(false)
	case *tg.InputMediaDocument:
		media.SetSpoiler(false)
	}
}

// PlanProgress describes resolved album members before any writes.
type PlanProgress interface{ OnPlan(Elem, []*tg.Message) }

// ResultProgress receives confirmed server IDs in the original request order.
type ResultProgress interface{ OnSent(Elem, []int) }

// StageProgress reports active byte-transfer operations, excluding queue waits.
type StageProgress interface{ OnStage(Elem, string, bool) }

// TransferProgress reports byte deltas separately for both directions.
type TransferProgress interface{ OnTransfer(Elem, string, int64) }

func (f *Forwarder) sentResult(elem Elem, result tg.UpdatesClass, randomIDs []int64) error {
	if f.opts.Duplex == nil || elem.AsDryRun() {
		return nil
	}
	ids, err := resultIDs(result, randomIDs)
	if err != nil {
		return err
	}
	if progress, ok := f.opts.Progress.(ResultProgress); ok {
		progress.OnSent(elem, ids)
	}
	return nil
}

func resultIDs(result tg.UpdatesClass, randomIDs []int64) ([]int, error) {
	var updates []tg.UpdateClass
	switch result := result.(type) {
	case *tg.Updates:
		updates = result.Updates
	case *tg.UpdatesCombined:
		updates = result.Updates
	case *tg.UpdateShortSentMessage:
		if len(randomIDs) == 1 {
			return []int{result.ID}, nil
		}
	}
	mapping := make(map[int64]int)
	for _, update := range updates {
		if id, ok := update.(*tg.UpdateMessageID); ok {
			mapping[id.RandomID] = id.ID
		}
	}
	ids := make([]int, len(randomIDs))
	for index, id := range randomIDs {
		if mapping[id] == 0 {
			return nil, fmt.Errorf("send acknowledgement lacks a confirmed message ID; check destination before retrying")
		}
		ids[index] = mapping[id]
	}
	return ids, nil
}
