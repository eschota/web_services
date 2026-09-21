# Drawflow 0.0.60

Vendored, not loaded from a CDN: these pages have no build step, and a node
editor that stops working because somebody else's CDN had a bad day is not
worth the saved kilobytes.

Source: https://github.com/jerosoler/Drawflow (MIT), files taken verbatim from
`drawflow@0.0.60/dist`.

Chosen over a canvas-drawn editor because its nodes are ordinary DOM: the
parameter controls inside a node are real `<select>` and `<input>` elements,
which is what makes them usable.
