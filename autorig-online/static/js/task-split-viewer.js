export const SPLIT_VIEW_IDS = Object.freeze(['perspective', 'top', 'front', 'left']);

export const DEFAULT_SPLIT_VIEW_STATE = Object.freeze({
    mainView: 'perspective',
    railViews: Object.freeze(['top', 'front', 'left']),
    maximizedView: null,
});

function validViewId(value) {
    return SPLIT_VIEW_IDS.includes(String(value || ''));
}

export function normalizeSplitViewportState(value = {}) {
    const mainView = validViewId(value.mainView) ? value.mainView : DEFAULT_SPLIT_VIEW_STATE.mainView;
    const requestedRail = Array.isArray(value.railViews) ? value.railViews : DEFAULT_SPLIT_VIEW_STATE.railViews;
    const railViews = requestedRail.filter(validViewId).filter((id, index, values) => values.indexOf(id) === index && id !== mainView);
    for (const id of SPLIT_VIEW_IDS) {
        if (id !== mainView && !railViews.includes(id)) railViews.push(id);
    }
    const maximizedView = validViewId(value.maximizedView) ? value.maximizedView : null;
    return {
        mainView,
        railViews: railViews.slice(0, 3),
        maximizedView,
    };
}

export function activateSplitViewport(state, viewId) {
    const current = normalizeSplitViewportState(state);
    if (!validViewId(viewId)) return current;

    if (current.maximizedView === viewId) {
        return { ...current, maximizedView: null };
    }

    if (current.maximizedView) {
        return current;
    }

    if (current.mainView === viewId) {
        return { ...current, maximizedView: viewId };
    }

    const railIndex = current.railViews.indexOf(viewId);
    if (railIndex < 0) return current;
    const railViews = [...current.railViews];
    railViews[railIndex] = current.mainView;
    return {
        mainView: viewId,
        railViews,
        maximizedView: viewId,
    };
}

export function splitViewportRects(state, width, height, mainRatio = 0.72) {
    const current = normalizeSplitViewportState(state);
    const w = Math.max(1, Number(width) || 1);
    const h = Math.max(1, Number(height) || 1);
    if (current.maximizedView) {
        return [{ id: current.maximizedView, x: 0, y: 0, width: w, height: h, role: 'maximized' }];
    }

    const ratio = Math.min(0.82, Math.max(0.58, Number(mainRatio) || 0.72));
    const mainWidth = Math.round(w * ratio);
    const railWidth = w - mainWidth;
    const rows = current.railViews.length || 3;
    const rects = [{ id: current.mainView, x: 0, y: 0, width: mainWidth, height: h, role: 'main' }];
    let rowTop = 0;
    current.railViews.forEach((id, index) => {
        const rowBottom = index === rows - 1 ? h : Math.round(((index + 1) * h) / rows);
        rects.push({
            id,
            x: mainWidth,
            y: rowTop,
            width: railWidth,
            height: rowBottom - rowTop,
            role: 'rail',
        });
        rowTop = rowBottom;
    });
    return rects;
}

export function splitViewportAtPoint(rects, x, y) {
    const px = Number(x);
    const py = Number(y);
    return (rects || []).find((rect) => (
        px >= rect.x && px < rect.x + rect.width && py >= rect.y && py < rect.y + rect.height
    )) || null;
}

export function splitViewportNdc(rect, x, y) {
    if (!rect) return { x: 0, y: 0 };
    const ndcY = -(((Number(y) - rect.y) / Math.max(1, rect.height)) * 2 - 1);
    return {
        x: ((Number(x) - rect.x) / Math.max(1, rect.width)) * 2 - 1,
        y: Object.is(ndcY, -0) ? 0 : ndcY,
    };
}

export function isSplitViewportControlTarget(target) {
    return Boolean(target?.closest?.('button, select, input, textarea, a, label, [role="button"], [data-split-viewport-no-activate]'));
}

export class TaskSplitViewportController {
    constructor(options = {}) {
        this.canvas = options.canvas || null;
        this.host = options.host || this.canvas?.parentElement || null;
        this.mainRatio = Number(options.mainRatio) || 0.72;
        this.dragThreshold = Math.max(2, Number(options.dragThreshold) || 6);
        this.state = normalizeSplitViewportState(options.initialState);
        this.onInteractionView = options.onInteractionView || (() => {});
        this.onStateChange = options.onStateChange || (() => {});
        this.pointerStart = null;
        this.interactionView = 'perspective';
        this.externalSurfaces = new Map();

        this._pointerDown = this._pointerDown.bind(this);
        this._pointerUp = this._pointerUp.bind(this);
        this._pointerCancel = this._pointerCancel.bind(this);
        this._wheel = this._wheel.bind(this);

        if (this.canvas) {
            this.canvas.addEventListener('pointerdown', this._pointerDown, true);
            this.canvas.addEventListener('pointerup', this._pointerUp, true);
            this.canvas.addEventListener('pointercancel', this._pointerCancel, true);
            this.canvas.addEventListener('pointerleave', this._pointerCancel, true);
            this.canvas.addEventListener('wheel', this._wheel, { capture: true, passive: true });
        }
    }

    destroy() {
        if (this.canvas) {
            this.canvas.removeEventListener('pointerdown', this._pointerDown, true);
            this.canvas.removeEventListener('pointerup', this._pointerUp, true);
            this.canvas.removeEventListener('pointercancel', this._pointerCancel, true);
            this.canvas.removeEventListener('pointerleave', this._pointerCancel, true);
            this.canvas.removeEventListener('wheel', this._wheel, true);
        }
        [...this.externalSurfaces.keys()].forEach((surface) => this.unregisterExternalSurface(surface));
    }

    registerExternalSurface(surface, viewId, options = {}) {
        if (!surface || !validViewId(viewId)) return () => {};
        this.unregisterExternalSurface(surface);
        const state = { pointerStart: null };
        const ignoreTarget = typeof options.ignoreTarget === 'function'
            ? options.ignoreTarget
            : isSplitViewportControlTarget;
        const pointerDown = (event) => {
            if (event.button !== 0 || ignoreTarget(event.target)) {
                state.pointerStart = null;
                return;
            }
            this.setInteractionView(viewId);
            state.pointerStart = {
                pointerId: event.pointerId,
                x: event.clientX,
                y: event.clientY,
            };
        };
        const pointerUp = (event) => {
            const start = state.pointerStart;
            state.pointerStart = null;
            if (!start || start.pointerId !== event.pointerId || ignoreTarget(event.target)) return;
            const distance = Math.hypot(event.clientX - start.x, event.clientY - start.y);
            if (distance <= this.dragThreshold) this.activate(viewId);
        };
        const pointerCancel = () => { state.pointerStart = null; };
        const wheel = () => this.setInteractionView(viewId);
        surface.addEventListener('pointerdown', pointerDown, true);
        surface.addEventListener('pointerup', pointerUp, true);
        surface.addEventListener('pointercancel', pointerCancel, true);
        surface.addEventListener('pointerleave', pointerCancel, true);
        surface.addEventListener('wheel', wheel, { capture: true, passive: true });
        this.externalSurfaces.set(surface, { pointerDown, pointerUp, pointerCancel, wheel });
        return () => this.unregisterExternalSurface(surface);
    }

    unregisterExternalSurface(surface) {
        const handlers = this.externalSurfaces.get(surface);
        if (!handlers) return;
        surface.removeEventListener('pointerdown', handlers.pointerDown, true);
        surface.removeEventListener('pointerup', handlers.pointerUp, true);
        surface.removeEventListener('pointercancel', handlers.pointerCancel, true);
        surface.removeEventListener('pointerleave', handlers.pointerCancel, true);
        surface.removeEventListener('wheel', handlers.wheel, true);
        this.externalSurfaces.delete(surface);
    }

    getRects() {
        return splitViewportRects(
            this.state,
            this.host?.clientWidth || this.canvas?.clientWidth || 1,
            this.host?.clientHeight || this.canvas?.clientHeight || 1,
            this.mainRatio,
        );
    }

    getRect(viewId) {
        return this.getRects().find((rect) => rect.id === viewId) || null;
    }

    getViewAtEvent(event) {
        const canvasRect = this.canvas?.getBoundingClientRect();
        if (!canvasRect) return null;
        const x = event.clientX - canvasRect.left;
        const y = event.clientY - canvasRect.top;
        return splitViewportAtPoint(this.getRects(), x, y);
    }

    getNdcForEvent(event, expectedViewId = null) {
        const canvasRect = this.canvas?.getBoundingClientRect();
        if (!canvasRect) return { x: 0, y: 0 };
        const rect = expectedViewId ? this.getRect(expectedViewId) : this.getViewAtEvent(event);
        return splitViewportNdc(rect, event.clientX - canvasRect.left, event.clientY - canvasRect.top);
    }

    isEventInView(event, viewId) {
        return this.getViewAtEvent(event)?.id === viewId;
    }

    isSplit() {
        return !this.state.maximizedView;
    }

    activeRenderView() {
        return this.state.maximizedView || this.state.mainView;
    }

    setInteractionView(viewId) {
        if (!validViewId(viewId)) return;
        this.interactionView = viewId;
        this.onInteractionView(viewId);
    }

    activate(viewId) {
        const next = activateSplitViewport(this.state, viewId);
        const changed = JSON.stringify(next) !== JSON.stringify(this.state);
        this.state = next;
        if (changed) this.onStateChange(this.state, this.getRects());
        return this.state;
    }

    refresh() {
        this.onStateChange(this.state, this.getRects());
    }

    _pointerDown(event) {
        if (event.button !== 0) return;
        const rect = this.getViewAtEvent(event);
        if (!rect) return;
        this.setInteractionView(rect.id);
        this.pointerStart = {
            pointerId: event.pointerId,
            viewId: rect.id,
            x: event.clientX,
            y: event.clientY,
        };
    }

    _pointerUp(event) {
        const start = this.pointerStart;
        this.pointerStart = null;
        if (!start || start.pointerId !== event.pointerId) return;
        const rect = this.getViewAtEvent(event);
        const distance = Math.hypot(event.clientX - start.x, event.clientY - start.y);
        if (rect?.id === start.viewId && distance <= this.dragThreshold) this.activate(start.viewId);
    }

    _pointerCancel() {
        this.pointerStart = null;
    }

    _wheel(event) {
        const rect = this.getViewAtEvent(event);
        if (rect) this.setInteractionView(rect.id);
    }
}
