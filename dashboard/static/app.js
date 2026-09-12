console.log("🏎️ [F1 Cockpit] app.js loaded v2");

function initApp() {
    console.log("🏎️ [F1 Cockpit] Initializing cockpit application...");
    // State Store
    const state = {
        selectedDriver: '16', // Default: Charles Leclerc
        drivers: {},          // Latest telemetry snapshot per driver: { "16": {...} }
        driverRoster: {},     // Metadata { code, name, team, color }
        trackPoints: [],      // Visited coordinates: [{x, y, color}]
        bounds: { minX: Infinity, maxX: -Infinity, minY: Infinity, maxY: -Infinity },
        totalEvents: 0,
        wsConnected: false
    };

    // DOM Elements
    const elements = {
        status: document.getElementById('connection-status'),
        statusText: document.querySelector('.status-text'),
        sessionInfo: document.getElementById('session-id'),
        timestamp: document.getElementById('telemetry-timestamp'),
        rosterBar: document.getElementById('driver-roster-bar'),
        trackCanvas: document.getElementById('trackCanvas'),
        activeCarsCount: document.getElementById('active-cars-count'),
        
        cockpitTitle: document.getElementById('cockpit-driver-title'),
        cockpitTeam: document.getElementById('cockpit-team-tag'),
        gear: document.getElementById('gauge-gear'),
        rpm: document.getElementById('gauge-rpm'),
        speed: document.getElementById('gauge-speed'),
        speedBar: document.getElementById('speed-bar'),
        throttlePct: document.getElementById('throttle-pct'),
        throttleBar: document.getElementById('throttle-bar'),
        brakePct: document.getElementById('brake-pct'),
        brakeBar: document.getElementById('brake-bar'),
        shiftLights: document.querySelectorAll('#shift-lights .light'),
        metaX: document.getElementById('meta-x'),
        metaY: document.getElementById('meta-y'),
        metaEvents: document.getElementById('meta-events'),
        leaderboardBody: document.getElementById('leaderboard-body')
    };

    // Initialize Canvas
    const ctx = elements.trackCanvas.getContext('2d');
    function resizeCanvas() {
        elements.trackCanvas.width = elements.trackCanvas.parentElement.clientWidth;
        elements.trackCanvas.height = elements.trackCanvas.parentElement.clientHeight;
    }
    window.addEventListener('resize', resizeCanvas);
    resizeCanvas();

    // Fetch initial driver roster from REST API
    async function loadDriverRoster() {
        try {
            const res = await fetch('/api/drivers');
            if (res.ok) {
                state.driverRoster = await res.json();
                renderDriverPills();
            }
        } catch (e) {
            console.warn('Could not load driver roster:', e);
        }
    }

    // Render Driver Filter Pills
    function renderDriverPills() {
        elements.rosterBar.innerHTML = '';
        
        // ALL Grid pill
        const allBtn = document.createElement('button');
        allBtn.className = `driver-pill ${state.selectedDriver === 'ALL' ? 'active' : ''}`;
        allBtn.dataset.driver = 'ALL';
        allBtn.innerHTML = `<span class="driver-num">ALL</span><span class="driver-code">GRID</span>`;
        allBtn.onclick = () => selectDriver('ALL');
        elements.rosterBar.appendChild(allBtn);

        // Individual drivers
        Object.entries(state.driverRoster).forEach(([num, info]) => {
            const btn = document.createElement('button');
            btn.className = `driver-pill ${state.selectedDriver === num ? 'active' : ''}`;
            btn.dataset.driver = num;
            btn.style.borderLeft = `4px solid ${info.color || '#FFF'}`;
            btn.innerHTML = `<span class="driver-num">#${num}</span><span class="driver-code">${info.code}</span>`;
            btn.onclick = () => selectDriver(num);
            elements.rosterBar.appendChild(btn);
        });
    }

    function selectDriver(driverNum) {
        state.selectedDriver = driverNum;
        document.querySelectorAll('.driver-pill').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.driver === driverNum);
        });
        updateCockpit();
    }

    // WebSocket Connection Management
    let ws = null;
    let reconnectTimeout = null;

    function connectWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws/telemetry`;

        elements.status.classList.remove('online');
        elements.statusText.textContent = 'CONNECTING...';

        ws = new WebSocket(wsUrl);

        ws.onopen = () => {
            state.wsConnected = true;
            elements.status.classList.add('online');
            elements.statusText.textContent = 'LIVE TELEMETRY STREAM';
            console.log('Connected to F1 Telemetry WebSocket');
        };

        ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                if (data.type === 'connection_ack') {
                    if (data.drivers) {
                        state.driverRoster = { ...state.driverRoster, ...data.drivers };
                        renderDriverPills();
                    }
                    return;
                }
                handleTelemetryEvent(data);
            } catch (err) {
                console.error('Error parsing telemetry payload:', err);
            }
        };

        ws.onclose = () => {
            state.wsConnected = false;
            elements.status.classList.remove('online');
            elements.statusText.textContent = 'DISCONNECTED (RECONNECTING...)';
            clearTimeout(reconnectTimeout);
            reconnectTimeout = setTimeout(connectWebSocket, 3000);
        };

        ws.onerror = () => {
            ws.close();
        };
    }

    // =========================================================================
    // Clock & Race Timestamp Management
    // =========================================================================
    let clockSeconds = null;
    let clockRunning = false;

    function parseTimeToSeconds(ts) {
        if (!ts || ts === 'NaT' || ts === 'None' || ts === 'null') return null;
        const str = String(ts).trim();

        // Check for HH:MM:SS or HH:MM:SS.mmm anywhere in string
        const match = str.match(/(\d{1,2}):(\d{2}):(\d{2})/);
        if (match) {
            const h = parseInt(match[1], 10);
            const m = parseInt(match[2], 10);
            const s = parseInt(match[3], 10);
            return h * 3600 + m * 60 + s;
        }

        const d = new Date(str);
        if (!isNaN(d.getTime())) {
            return d.getHours() * 3600 + d.getMinutes() * 60 + d.getSeconds();
        }
        return null;
    }

    function formatSecondsToClock(totalSeconds) {
        if (totalSeconds === null || isNaN(totalSeconds)) {
            const now = new Date();
            const h = String(now.getHours()).padStart(2, '0');
            const m = String(now.getMinutes()).padStart(2, '0');
            const s = String(now.getSeconds()).padStart(2, '0');
            return `${h}:${m}:${s}`;
        }
        const sMod = ((totalSeconds % 86400) + 86400) % 86400;
        const h = String(Math.floor(sMod / 3600)).padStart(2, '0');
        const m = String(Math.floor((sMod % 3600) / 60)).padStart(2, '0');
        const s = String(sMod % 60).padStart(2, '0');
        return `${h}:${m}:${s}`;
    }

    function formatTimeToSeconds(ts) {
        console.log('formatting to secs', ts)
        const sec = parseTimeToSeconds(ts);
        if (sec !== null) {
            return formatSecondsToClock(sec);
        }
        return '--:--:--';
    }

    function startClock() {
        if (clockRunning) return;
        clockRunning = true;

        function tick() {
            if (clockSeconds !== null) {
                clockSeconds += 1;
                console.log(clockSeconds);
                elements.timestamp.textContent = formatSecondsToClock(clockSeconds);
            } else {
                const now = new Date();
                const h = String(now.getHours()).padStart(2, '0');
                const m = String(now.getMinutes()).padStart(2, '0');
                const s = String(now.getSeconds()).padStart(2, '0');
                console.log(h, m, s);
                elements.timestamp.textContent = `${h}:${m}:${s}`;
            }
        }
        tick();
        setInterval(tick, 1000);
    }

    // Handle Incoming Telemetry Message
    function handleTelemetryEvent(payload) {
        state.totalEvents++;
        elements.metaEvents.textContent = state.totalEvents.toLocaleString();

        const driverNum = String(payload.driver_number || '');
        if (!driverNum) return;

        // Enrich with driver roster metadata if present
        const rosterMeta = state.driverRoster[driverNum] || {};
        const teamColor = payload.team_color || rosterMeta.color || '#00D2BE';
        const driverCode = payload.driver_code || rosterMeta.code || `D${driverNum}`;
        const teamName = payload.team_name || rosterMeta.team || 'Formula 1';

        // Update driver snapshot
        state.drivers[driverNum] = {
            ...payload,
            driver_code: driverCode,
            team_name: teamName,
            team_color: teamColor,
            last_seen: new Date()
        };

        // Initialize base race clock on first valid timestamp (data updates, but does NOT overwrite clock)
        if (clockSeconds === null && payload.timestamp) {
            const initialSec = parseTimeToSeconds(payload.timestamp);
            if (initialSec !== null) {
                clockSeconds = initialSec;
                console.log('initial data', initialSec)
                elements.timestamp.textContent = formatSecondsToClock(clockSeconds);
            }
        }

        if (payload.session_id) {
            elements.sessionInfo.textContent = payload.session_id;
        }

        // Track coordinate recording for canvas mapping
        const x = Number(payload.x_pos || 0);
        const y = Number(payload.y_pos || 0);
        if (x !== 0 || y !== 0) {
            state.bounds.minX = Math.min(state.bounds.minX, x);
            state.bounds.maxX = Math.max(state.bounds.maxX, x);
            state.bounds.minY = Math.min(state.bounds.minY, y);
            state.bounds.maxY = Math.max(state.bounds.maxY, y);

            // Record track trace point
            if (state.trackPoints.length < 3000) {
                state.trackPoints.push({ x, y });
            }
        }

        // If this event matches the selected driver (or none chosen yet), update cockpit immediately
        if (state.selectedDriver === driverNum || (state.selectedDriver === 'ALL' && driverNum === '16')) {
            updateCockpit(state.drivers[driverNum]);
        }

        updateLeaderboard();
        elements.activeCarsCount.textContent = `Cars on track: ${Object.keys(state.drivers).length}`;
    }

    // Update Cockpit Gauges
    function updateCockpit(driverData = null) {
        let data = driverData;
        if (!data) {
            if (state.selectedDriver === 'ALL') {
                const firstDriver = Object.keys(state.drivers)[0];
                data = state.drivers[firstDriver];
            } else {
                data = state.drivers[state.selectedDriver];
            }
        }

        if (!data) return;

        // Driver title & team
        const code = data.driver_code || data.driver_number;
        const team = data.team_name || 'Formula 1';
        elements.cockpitTitle.textContent = `FOCUSED COCKPIT: #${data.driver_number} ${code} (${team.toUpperCase()})`;
        elements.cockpitTeam.textContent = team.toUpperCase();
        elements.cockpitTeam.style.backgroundColor = data.team_color ? `${data.team_color}33` : 'rgba(255, 255, 255, 0.1)';
        elements.cockpitTeam.style.color = data.team_color || '#FFF';

        // Gear
        const gear = data.gear;
        elements.gear.textContent = gear === 0 ? 'N' : (gear === -1 ? 'R' : gear);

        // RPM
        const rpm = data.rpm || 0;
        elements.rpm.textContent = `${rpm.toLocaleString()} RPM`;
        updateShiftLights(rpm);

        // Speed (0-360 km/h)
        const speed = Math.max(0, Math.round(data.speed_kmh || 0));
        elements.speed.textContent = speed;
        const speedPct = Math.min(100, (speed / 360) * 100);
        elements.speedBar.style.width = `${speedPct}%`;

        // Throttle (0-100%)
        const throttle = Math.max(0, Math.min(100, Math.round(data.throttle || 0)));
        elements.throttlePct.textContent = `${throttle}%`;
        elements.throttleBar.style.width = `${throttle}%`;

        // Brake (0-100%)
        const brake = Math.max(0, Math.min(100, Math.round(data.brake || 0)));
        elements.brakePct.textContent = `${brake}%`;
        elements.brakeBar.style.width = `${brake}%`;

        // Coordinates
        elements.metaX.textContent = data.x_pos || 0;
        elements.metaY.textContent = data.y_pos || 0;
    }

    // Shift Lights (RPM LED Tachometer)
    function updateShiftLights(rpm) {
        // F1 V6 Turbo Hybrid rev limit ~ 12,500 RPM
        const totalLights = elements.shiftLights.length; // 8
        const minRpm = 8500;
        const maxRpm = 12200;

        let activeCount = 0;
        if (rpm > minRpm) {
            const ratio = (rpm - minRpm) / (maxRpm - minRpm);
            activeCount = Math.min(totalLights, Math.floor(ratio * totalLights) + 1);
        }

        elements.shiftLights.forEach((light, idx) => {
            light.classList.toggle('active', idx < activeCount);
        });
    }

    // Update Grid Leaderboard Table
    function updateLeaderboard() {
        const sortedDrivers = Object.values(state.drivers).sort((a, b) => (b.speed_kmh || 0) - (a.speed_kmh || 0));
        
        let html = '';
        sortedDrivers.forEach(d => {
            const isSelected = state.selectedDriver === String(d.driver_number);
            const color = d.team_color || '#FFF';
            html += `
                <tr style="${isSelected ? 'background: rgba(0, 210, 190, 0.08); font-weight: 700;' : ''}">
                    <td>
                        <span class="car-badge" style="background: ${color}22; color: ${color}; border: 1px solid ${color}66;">
                            #${d.driver_number}
                        </span>
                    </td>
                    <td>${d.driver_code} (${d.driver_name || ''})</td>
                    <td style="color: ${color}; font-weight: 600;">${d.team_name}</td>
                    <td><strong>${d.speed_kmh}</strong> km/h</td>
                    <td>${d.gear === 0 ? 'N' : d.gear}</td>
                    <td>${d.rpm}</td>
                    <td style="color: var(--throttle-color);">${d.throttle}%</td>
                    <td style="color: var(--brake-color);">${d.brake}%</td>
                    <td style="color: var(--text-muted); font-size: 0.8rem;">${d.timestamp ? formatTimeToSeconds(d.timestamp) : ''}</td>
                </tr>
            `;
        });
        elements.leaderboardBody.innerHTML = html;
    }

    // 2D Canvas Track Renderer Loop
    function renderTrack() {
        const width = elements.trackCanvas.width;
        const height = elements.trackCanvas.height;
        if (!width || !height) return;

        ctx.clearRect(0, 0, width, height);

        // Draw background grid lines
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.03)';
        ctx.lineWidth = 1;
        const gridSize = 40;
        for (let x = 0; x < width; x += gridSize) {
            ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, height); ctx.stroke();
        }
        for (let y = 0; y < height; y += gridSize) {
            ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(width, y); ctx.stroke();
        }

        const b = state.bounds;
        const padding = 50;
        const spanX = (b.maxX - b.minX) || 1;
        const spanY = (b.maxY - b.minY) || 1;

        const scaleX = (width - padding * 2) / spanX;
        const scaleY = (height - padding * 2) / spanY;
        const scale = Math.min(scaleX, scaleY) || 1;

        const offsetX = (width - spanX * scale) / 2;
        const offsetY = (height - spanY * scale) / 2;

        function toScreen(gx, gy) {
            return {
                sx: offsetX + (gx - b.minX) * scale,
                // Invert Y coordinate so orientation matches real race track layout
                sy: height - (offsetY + (gy - b.minY) * scale)
            };
        }

        // Draw circuit path trace
        if (state.trackPoints.length > 2) {
            ctx.strokeStyle = 'rgba(255, 255, 255, 0.25)';
            ctx.lineWidth = 3;
            ctx.lineCap = 'round';
            ctx.beginPath();
            const first = toScreen(state.trackPoints[0].x, state.trackPoints[0].y);
            ctx.moveTo(first.sx, first.sy);
            for (let i = 1; i < state.trackPoints.length; i++) {
                const pt = toScreen(state.trackPoints[i].x, state.trackPoints[i].y);
                ctx.lineTo(pt.sx, pt.sy);
            }
            ctx.stroke();
        }

        // Draw cars on track
        Object.values(state.drivers).forEach(d => {
            const x = Number(d.x_pos || 0);
            const y = Number(d.y_pos || 0);
            if (x === 0 && y === 0) return;

            const pos = toScreen(x, y);
            const color = d.team_color || '#FFF';
            const isSelected = state.selectedDriver === String(d.driver_number);

            // Car Blip Glow
            ctx.shadowColor = color;
            ctx.shadowBlur = isSelected ? 20 : 10;

            // Outer Pulse Ring for Selected Driver
            if (isSelected) {
                ctx.strokeStyle = color;
                ctx.lineWidth = 2;
                ctx.beginPath();
                ctx.arc(pos.sx, pos.sy, 16, 0, Math.PI * 2);
                ctx.stroke();
            }

            // Car Core Dot
            ctx.fillStyle = color;
            ctx.beginPath();
            ctx.arc(pos.sx, pos.sy, isSelected ? 8 : 6, 0, Math.PI * 2);
            ctx.fill();

            // Label Tag (#Number Code)
            ctx.shadowBlur = 0;
            ctx.font = isSelected ? 'bold 12px Rajdhani' : '10px Rajdhani';
            ctx.fillStyle = '#FFFFFF';
            ctx.fillText(`${d.driver_code}`, pos.sx + 10, pos.sy - 8);
        });

        requestAnimationFrame(renderTrack);
    }

    // Start App
    loadDriverRoster();
    startClock();
    connectWebSocket();
    requestAnimationFrame(renderTrack);
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initApp);
} else {
    initApp();
}
