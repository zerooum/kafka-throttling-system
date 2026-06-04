// chart.js — fixed-capacity sample buffer + canvas sparkline renderer.
// Pure data logic (push/samples/max) is independently testable; draw() needs a
// canvas 2D context and is exercised by the live UI.
export class Sparkline {
  constructor(capacity = 60) {
    this.capacity = capacity;
    this.refLine = 0; // reference value (msg/s ceiling); set by the caller
    this._data = [];
  }

  push(v) {
    this._data.push(v);
    if (this._data.length > this.capacity) this._data.shift();
  }

  samples() {
    return this._data.slice();
  }

  max() {
    return this._data.length ? Math.max(...this._data) : 0;
  }

  // Draw the series and a dashed reference line into a 2D context.
  draw(ctx, width, height, refLine) {
    ctx.clearRect(0, 0, width, height);
    const data = this._data;
    const peak = Math.max(this.max(), refLine || 0, 1);
    const pad = 4;
    const w = width - pad * 2;
    const h = height - pad * 2;
    const x = (i) => pad + (data.length <= 1 ? 0 : (i / (data.length - 1)) * w);
    const y = (v) => pad + h - (v / peak) * h;

    if (refLine) {
      ctx.strokeStyle = "#c0392b";
      ctx.setLineDash([4, 4]);
      ctx.beginPath();
      ctx.moveTo(pad, y(refLine));
      ctx.lineTo(pad + w, y(refLine));
      ctx.stroke();
      ctx.setLineDash([]);
    }

    ctx.strokeStyle = "#2ecc71";
    ctx.lineWidth = 2;
    ctx.beginPath();
    data.forEach((v, i) => {
      const px = x(i);
      const py = y(v);
      if (i === 0) ctx.moveTo(px, py);
      else ctx.lineTo(px, py);
    });
    ctx.stroke();
  }
}
