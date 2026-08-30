(() => {
  'use strict';

  const SYMBOL = 'XRPUSDT';
  const COOLDOWN_MS = 2 * 60 * 1000;
  const WATCHDOG_MS = 45 * 1000;
  const RECONNECT_MIN_MS = 1000;
  const RECONNECT_MAX_MS = 30000;

  const REST_URL = `https://api.binance.com/api/v3/ticker/24hr?symbol=${SYMBOL}`;
  const WS_URL = `wss://stream.binance.com:9443/ws/${SYMBOL.toLowerCase()}@ticker`;

  const els = {
    price: document.getElementById('price'),
    high: document.getElementById('high'),
    low: document.getElementById('low'),
    lastHit: document.getElementById('last-hit'),
    cooldown: document.getElementById('cooldown'),
    lastUpdate: document.getElementById('last-update'),
    streamState: document.getElementById('stream-state'),
    status: document.getElementById('status')
  };

  const highAudio = document.getElementById('high-sound');
  const lowAudio = document.getElementById('low-sound');

  let socket = null;
  let reconnectTimer = null;
  let watchdogTimer = null;
  let reconnectDelay = RECONNECT_MIN_MS;
  let lastMessageAt = 0;
  let initialized = false;
  let high = null;
  let low = null;
  let price = null;

  // Separate cooldowns. A hit also requires the price to leave the level
  // before that side can arm again, preventing repeated alarms while price
  // remains at the same extreme.
  const alertState = {
    high: { cooldownUntil: 0, armed: true },
    low: { cooldownUntil: 0, armed: true }
  };

  function setStatus(type, text) {
    els.status.className = `status ${type}`;
    els.status.querySelector('span:last-child').textContent = text;
  }

  function formatPrice(value) {
    if (!Number.isFinite(value)) return '--';
    return value.toFixed(6).replace(/0+$/, '').replace(/\.$/, '');
  }

  function formatTime(ms) {
    if (!ms) return '--';
    return new Date(ms).toLocaleTimeString([], { hour12: false });
  }

  function render() {
    els.price.textContent = formatPrice(price);
    els.high.textContent = formatPrice(high);
    els.low.textContent = formatPrice(low);

    const now = Date.now();
    const highRemaining = Math.max(0, alertState.high.cooldownUntil - now);
    const lowRemaining = Math.max(0, alertState.low.cooldownUntil - now);

    if (highRemaining > 0 || lowRemaining > 0) {
      const parts = [];
      if (highRemaining > 0) parts.push(`H ${Math.ceil(highRemaining / 1000)}s`);
      if (lowRemaining > 0) parts.push(`L ${Math.ceil(lowRemaining / 1000)}s`);
      els.cooldown.textContent = parts.join('  ');
    } else {
      els.cooldown.textContent = 'READY';
    }

    if (lastMessageAt) els.lastUpdate.textContent = formatTime(lastMessageAt);
  }

  function setLastHit(side, hitPrice, timestamp) {
    const arrow = side === 'HIGH' ? '▲' : '▼';
    els.lastHit.textContent = `${arrow} ${side}  ${formatPrice(hitPrice)}  ${formatTime(timestamp)}`;
  }

  async function playAudio(audio) {
    try {
      audio.currentTime = 0;
      await audio.play();
    } catch (error) {
      // Browser autoplay restrictions can reject play(). OBS Browser Source
      // normally permits this use case; keep monitoring alive if audio fails.
      console.warn('Audio playback was blocked:', error);
    }
  }

  function trigger(side) {
    const state = alertState[side.toLowerCase()];
    const now = Date.now();

    if (!state.armed || now < state.cooldownUntil) return;

    state.armed = false;
    state.cooldownUntil = now + COOLDOWN_MS;
    setLastHit(side, price, now);

    if (side === 'HIGH') playAudio(highAudio);
    else playAudio(lowAudio);

    render();
  }

  function processPrice(nextPrice) {
    if (!Number.isFinite(nextPrice)) return;
    price = nextPrice;

    // Re-arm only after price has moved away from the relevant extreme.
    if (high !== null && price < high) alertState.high.armed = true;
    if (low !== null && price > low) alertState.low.armed = true;

    // With the current rolling 24h ticker, high/low are supplied by Binance.
    // If the live price reaches either current boundary, trigger once.
    if (initialized) {
      if (high !== null && price >= high) trigger('HIGH');
      if (low !== null && price <= low) trigger('LOW');
    }

    render();
  }

  function update24hFromTicker(data, isInitial = false) {
    const nextHigh = Number(data.highPrice);
    const nextLow = Number(data.lowPrice);
    const nextPrice = Number(data.lastPrice ?? data.c);

    if (Number.isFinite(nextHigh)) high = nextHigh;
    if (Number.isFinite(nextLow)) low = nextLow;

    if (isInitial) {
      price = Number.isFinite(nextPrice) ? nextPrice : price;
      initialized = true;
      // Never alarm merely because the initial REST snapshot is sitting on a
      // boundary. Live WebSocket events must cause subsequent alerts.
      render();
    } else if (Number.isFinite(nextPrice)) {
      processPrice(nextPrice);
    }
  }

  async function initialize() {
    setStatus('reconnecting', 'INITIALIZING');
    els.streamState.textContent = 'REST SNAPSHOT';

    try {
      const response = await fetch(REST_URL, { cache: 'no-store' });
      if (!response.ok) throw new Error(`REST HTTP ${response.status}`);
      const data = await response.json();
      update24hFromTicker(data, true);
    } catch (error) {
      console.error('Initial Binance REST request failed:', error);
      setStatus('error', 'REST ERROR');
      els.streamState.textContent = 'REST ERROR';
      // Do not stop. WebSocket connection is still attempted.
    }

    connectWebSocket();
  }

  function clearReconnectTimer() {
    if (reconnectTimer !== null) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
  }

  function clearWatchdog() {
    if (watchdogTimer !== null) {
      clearTimeout(watchdogTimer);
      watchdogTimer = null;
    }
  }

  function armWatchdog() {
    clearWatchdog();
    watchdogTimer = setTimeout(() => {
      if (Date.now() - lastMessageAt >= WATCHDOG_MS) {
        console.warn('Binance WebSocket watchdog expired; reconnecting.');
        closeSocketAndReconnect();
      }
    }, WATCHDOG_MS);
  }

  function scheduleReconnect() {
    clearReconnectTimer();
    setStatus('reconnecting', 'RECONNECTING');
    els.streamState.textContent = `RECONNECT IN ${Math.ceil(reconnectDelay / 1000)}s`;

    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connectWebSocket();
    }, reconnectDelay);

    reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX_MS);
  }

  function closeSocketAndReconnect() {
    clearWatchdog();
    if (socket) {
      const old = socket;
      socket = null;
      try { old.close(); } catch (_) {}
    }
    scheduleReconnect();
  }

  function connectWebSocket() {
    clearReconnectTimer();

    // Never create a second active socket.
    if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) {
      return;
    }

    setStatus('reconnecting', 'CONNECTING');
    els.streamState.textContent = 'CONNECTING';

    let ws;
    try {
      ws = new WebSocket(WS_URL);
    } catch (error) {
      console.error('WebSocket construction failed:', error);
      scheduleReconnect();
      return;
    }

    socket = ws;

    ws.addEventListener('open', () => {
      if (socket !== ws) return;
      reconnectDelay = RECONNECT_MIN_MS;
      lastMessageAt = Date.now();
      setStatus('connected', 'CONNECTED');
      els.streamState.textContent = 'LIVE';
      armWatchdog();
    });

    ws.addEventListener('message', (event) => {
      if (socket !== ws) return;
      lastMessageAt = Date.now();
      armWatchdog();

      try {
        const data = JSON.parse(event.data);
        update24hFromTicker(data, false);
      } catch (error) {
        console.warn('Invalid Binance message:', error);
      }
    });

    ws.addEventListener('error', (error) => {
      if (socket !== ws) return;
      console.warn('Binance WebSocket error:', error);
      els.streamState.textContent = 'SOCKET ERROR';
    });

    ws.addEventListener('close', () => {
      if (socket !== ws) return;
      socket = null;
      clearWatchdog();
      scheduleReconnect();
    });
  }

  // Only a rendering interval; it never creates connections or alert timers.
  // This keeps the cooldown countdown visible without accumulating timers.
  setInterval(render, 250);

  window.addEventListener('beforeunload', () => {
    clearReconnectTimer();
    clearWatchdog();
    if (socket) {
      try { socket.close(); } catch (_) {}
      socket = null;
    }
  });

  initialize();
})();
