/* ── QueryPad Frontend ─────────────────────────────────────── */

let currentNotebook = null;
let connections = [];
let activeConnectionId = "";
let cellCounter = 0;

/* ── Init ─────────────────────────────────────────────────── */

document.addEventListener("DOMContentLoaded", async () => {
    await loadConnections();
    await loadNotebooks();
});

/* ── Connections ──────────────────────────────────────────── */

async function loadConnections() {
    const res = await fetch("/api/connections");
    connections = await res.json();
    renderConnectionSelect();
}

function renderConnectionSelect() {
    const sel = document.getElementById("active-connection");
    sel.innerHTML = '<option value="">-- No Connection --</option>';
    connections.forEach(c => {
        sel.innerHTML += `<option value="${c.id}" ${c.id === activeConnectionId ? "selected" : ""}>${c.name} (${c.db_type})</option>`;
    });
}

function onConnectionChange() {
    activeConnectionId = document.getElementById("active-connection").value;
    if (activeConnectionId) loadSchema();
    else document.getElementById("schema-tree").innerHTML = "";
}

function showConnectionModal() {
    document.getElementById("modal-root").innerHTML = `
        <div class="modal-overlay" onclick="if(event.target===this)closeModal()">
            <div class="modal">
                <h2>Add Database Connection</h2>
                <div class="form-group">
                    <label>Name</label>
                    <input type="text" id="conn-name" placeholder="My Database">
                </div>
                <div class="form-group">
                    <label>Connection URL</label>
                    <input type="text" id="conn-url" placeholder="sqlite:///data.db">
                    <div style="font-size:11px;color:var(--text-dim);margin-top:4px">
                        Examples: sqlite:///file.db, postgresql://user:pass@host/db, mysql+pymysql://user:pass@host/db
                    </div>
                </div>
                <div class="btn-row">
                    <button class="btn" onclick="closeModal()">Cancel</button>
                    <button class="btn btn-primary" onclick="addConnection()">Connect</button>
                </div>
            </div>
        </div>
    `;
}

async function addConnection() {
    const name = document.getElementById("conn-name").value || "Database";
    const url = document.getElementById("conn-url").value;
    if (!url) { alert("Connection URL is required"); return; }

    try {
        const res = await fetch("/api/connections", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name, url }),
        });
        const data = await res.json();
        if (data.error) { alert(data.error); return; }
        activeConnectionId = data.id;
        await loadConnections();
        await loadSchema();
        closeModal();
    } catch (err) {
        alert("Connection failed: " + err.message);
    }
}

/* ── Schema tree ──────────────────────────────────────────── */

async function loadSchema() {
    if (!activeConnectionId) return;
    const res = await fetch(`/api/connections/${activeConnectionId}/tables`);
    const tables = await res.json();
    renderSchemaTree(tables);
}

function renderSchemaTree(tables) {
    const container = document.getElementById("schema-tree");
    if (tables.length === 0) {
        container.innerHTML = '<div style="color:var(--text-dim);font-size:12px">No tables found</div>';
        return;
    }

    let html = "";
    tables.forEach((t, i) => {
        html += `<div class="table-name" onclick="toggleColumns(${i})">${t.name}</div>`;
        html += `<div class="column-list" id="cols-${i}">`;
        t.columns.forEach(c => {
            html += `<div class="column-item">${c.name}<span class="column-type">${c.type}</span></div>`;
        });
        html += `</div>`;
    });
    container.innerHTML = html;
}

function toggleColumns(index) {
    const el = document.getElementById(`cols-${index}`);
    el.classList.toggle("open");
}

/* ── Notebooks ────────────────────────────────────────────── */

async function loadNotebooks() {
    const res = await fetch("/api/notebooks");
    const notebooks = await res.json();
    renderNotebooksList(notebooks);
}

function renderNotebooksList(notebooks) {
    const container = document.getElementById("notebooks-list");
    if (notebooks.length === 0) {
        container.innerHTML = '<div style="color:var(--text-dim);font-size:12px">No notebooks yet</div>';
        return;
    }

    let html = "";
    notebooks.forEach(nb => {
        const isActive = currentNotebook && currentNotebook.id === nb.id;
        html += `<div class="sidebar-item ${isActive ? 'active' : ''}" onclick="openNotebook('${nb.id}')">
            <span>${nb.name}</span>
            <span style="font-size:11px;color:var(--text-dim)">${nb.cells_count} cells</span>
        </div>`;
    });
    container.innerHTML = html;
}

function showNewNotebookModal() {
    document.getElementById("modal-root").innerHTML = `
        <div class="modal-overlay" onclick="if(event.target===this)closeModal()">
            <div class="modal">
                <h2>New Notebook</h2>
                <div class="form-group">
                    <label>Name</label>
                    <input type="text" id="nb-name" placeholder="Analysis Notebook">
                </div>
                <div class="btn-row">
                    <button class="btn" onclick="closeModal()">Cancel</button>
                    <button class="btn btn-primary" onclick="createNotebook()">Create</button>
                </div>
            </div>
        </div>
    `;
}

async function createNotebook() {
    const name = document.getElementById("nb-name").value || "Untitled";
    const res = await fetch("/api/notebooks", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name, connection_id: activeConnectionId }),
    });
    const nb = await res.json();
    closeModal();
    await loadNotebooks();
    await openNotebook(nb.id);
}

async function openNotebook(nbId) {
    const res = await fetch(`/api/notebooks/${nbId}`);
    currentNotebook = await res.json();
    cellCounter = currentNotebook.cells.length;

    document.getElementById("notebook-title").textContent = currentNotebook.name;
    document.getElementById("empty-state").style.display = "none";
    document.getElementById("cells-container").style.display = "block";
    document.getElementById("add-cell-bar").style.display = "flex";

    if (currentNotebook.default_connection) {
        activeConnectionId = currentNotebook.default_connection;
        document.getElementById("active-connection").value = activeConnectionId;
        loadSchema();
    }

    renderCells();
    loadNotebooks();
}

async function saveNotebook() {
    if (!currentNotebook) return;
    collectCellData();
    await fetch(`/api/notebooks/${currentNotebook.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            name: currentNotebook.name,
            cells: currentNotebook.cells,
            default_connection: activeConnectionId,
            created_at: currentNotebook.created_at,
        }),
    });
}

/* ── Cell rendering ───────────────────────────────────────── */

function renderCells() {
    const container = document.getElementById("cells-container");
    container.innerHTML = "";

    currentNotebook.cells.forEach((cell, i) => {
        container.appendChild(createCellElement(cell, i));
    });
}

function createCellElement(cell, index) {
    const div = document.createElement("div");
    div.className = "cell";
    div.id = `cell-${cell.id}`;
    div.dataset.index = index;

    if (cell.cell_type === "sql") {
        div.innerHTML = `
            <div class="cell-header">
                <span class="cell-type sql">SQL</span>
                <div class="cell-actions">
                    <button class="btn btn-sm btn-success" onclick="runCell(${index})">Run (Ctrl+Enter)</button>
                    <button class="btn btn-sm" onclick="visualizeCell(${index})">Chart</button>
                    <button class="btn btn-sm btn-danger" onclick="removeCell(${index})">x</button>
                </div>
            </div>
            <textarea class="cell-editor" id="editor-${cell.id}"
                onkeydown="handleEditorKey(event, ${index})"
                placeholder="SELECT * FROM ...">${cell.source}</textarea>
            <div class="cell-result" id="result-${cell.id}"></div>
        `;
    } else if (cell.cell_type === "ai") {
        div.innerHTML = `
            <div class="cell-header">
                <span class="cell-type ai">AI Assistant</span>
                <div class="cell-actions">
                    <button class="btn btn-sm btn-danger" onclick="removeCell(${index})">x</button>
                </div>
            </div>
            <div class="ai-prompt">
                <input type="text" id="ai-input-${cell.id}"
                    placeholder="Ask in natural language: e.g. 'Show top 10 customers by revenue'"
                    value="${cell.source}"
                    onkeydown="if(event.key==='Enter')aiGenerate(${index})">
                <button class="btn btn-sm btn-primary" onclick="aiGenerate(${index})">Generate SQL</button>
            </div>
            <div class="ai-result" id="ai-result-${cell.id}"></div>
            <div class="cell-result" id="result-${cell.id}"></div>
        `;
    } else {
        div.innerHTML = `
            <div class="cell-header">
                <span class="cell-type markdown">Markdown</span>
                <div class="cell-actions">
                    <button class="btn btn-sm" onclick="toggleMarkdown(${index})">Preview</button>
                    <button class="btn btn-sm btn-danger" onclick="removeCell(${index})">x</button>
                </div>
            </div>
            <textarea class="cell-editor markdown-editor" id="editor-${cell.id}"
                placeholder="Write notes in Markdown...">${cell.source}</textarea>
            <div class="markdown-rendered" id="preview-${cell.id}" style="display:none"></div>
        `;
    }

    // Restore previous result if any
    if (cell.result && cell.cell_type !== "markdown") {
        setTimeout(() => renderResult(cell.id, cell.result), 0);
    }

    return div;
}

function addCell(type) {
    cellCounter++;
    const cell = {
        id: `c${cellCounter}_${Date.now()}`,
        cell_type: type,
        source: "",
        result: null,
        connection_id: activeConnectionId,
    };
    currentNotebook.cells.push(cell);
    const container = document.getElementById("cells-container");
    container.appendChild(createCellElement(cell, currentNotebook.cells.length - 1));
}

function removeCell(index) {
    currentNotebook.cells.splice(index, 1);
    renderCells();
}

function collectCellData() {
    currentNotebook.cells.forEach(cell => {
        if (cell.cell_type === "sql" || cell.cell_type === "markdown") {
            const editor = document.getElementById(`editor-${cell.id}`);
            if (editor) cell.source = editor.value;
        } else if (cell.cell_type === "ai") {
            const input = document.getElementById(`ai-input-${cell.id}`);
            if (input) cell.source = input.value;
        }
    });
}

/* ── Run cell ─────────────────────────────────────────────── */

function handleEditorKey(event, index) {
    if (event.ctrlKey && event.key === "Enter") {
        event.preventDefault();
        runCell(index);
    }
    // Tab support
    if (event.key === "Tab") {
        event.preventDefault();
        const el = event.target;
        const start = el.selectionStart;
        el.value = el.value.substring(0, start) + "  " + el.value.substring(el.selectionEnd);
        el.selectionStart = el.selectionEnd = start + 2;
    }
}

async function runCell(index) {
    if (!activeConnectionId) { alert("Select a database connection first"); return; }

    const cell = currentNotebook.cells[index];
    let sql = "";

    if (cell.cell_type === "sql") {
        const editor = document.getElementById(`editor-${cell.id}`);
        sql = editor.value.trim();
    } else if (cell.cell_type === "ai") {
        // Use previously generated SQL
        sql = cell._generatedSql || "";
    }

    if (!sql) return;

    const resultDiv = document.getElementById(`result-${cell.id}`);
    resultDiv.innerHTML = '<div style="padding:10px;color:var(--text-dim)">Running...</div>';

    try {
        const res = await fetch("/api/query", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ connection_id: activeConnectionId, sql, limit: 500 }),
        });
        const data = await res.json();
        cell.result = data;
        renderResult(cell.id, data);
    } catch (err) {
        resultDiv.innerHTML = `<div class="cell-error">${err.message}</div>`;
    }
}

async function runAllCells() {
    for (let i = 0; i < currentNotebook.cells.length; i++) {
        const cell = currentNotebook.cells[i];
        if (cell.cell_type === "sql" || (cell.cell_type === "ai" && cell._generatedSql)) {
            await runCell(i);
        }
    }
}

function renderResult(cellId, data) {
    const div = document.getElementById(`result-${cellId}`);
    if (!div) return;

    if (data.error) {
        div.innerHTML = `<div class="cell-error">${escapeHtml(data.error)}</div>`;
        return;
    }

    if (!data.columns || data.columns.length === 0) {
        div.innerHTML = `<div class="cell-stats"><span class="ok">Query executed</span><span>${data.row_count} rows affected</span><span>${data.elapsed_ms}ms</span></div>`;
        return;
    }

    let html = '<table class="data-table"><thead><tr>';
    data.columns.forEach(c => html += `<th>${escapeHtml(c)}</th>`);
    html += '</tr></thead><tbody>';

    data.rows.forEach(row => {
        html += '<tr>';
        data.columns.forEach(c => {
            const val = row[c] !== null && row[c] !== undefined ? row[c] : '';
            html += `<td>${escapeHtml(String(val))}</td>`;
        });
        html += '</tr>';
    });

    html += '</tbody></table>';
    html += `<div class="cell-stats">
        <span class="ok">${data.row_count} rows</span>
        <span>${data.columns.length} columns</span>
        <span>${data.elapsed_ms}ms</span>
        ${data.truncated ? '<span style="color:var(--orange)">truncated</span>' : ''}
    </div>`;

    div.innerHTML = html;
}

/* ── AI generation ────────────────────────────────────────── */

async function aiGenerate(index) {
    if (!activeConnectionId) { alert("Select a database connection first"); return; }

    const cell = currentNotebook.cells[index];
    const input = document.getElementById(`ai-input-${cell.id}`);
    const question = input.value.trim();
    if (!question) return;

    const resultDiv = document.getElementById(`ai-result-${cell.id}`);
    resultDiv.innerHTML = '<span style="color:var(--text-dim)">Generating SQL...</span>';

    try {
        const res = await fetch("/api/ai/generate", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ connection_id: activeConnectionId, question }),
        });
        const data = await res.json();

        if (data.error) {
            resultDiv.innerHTML = `<span style="color:var(--red)">${escapeHtml(data.error)}</span>`;
            return;
        }

        cell._generatedSql = data.sql;
        resultDiv.innerHTML = `
            <div style="margin-bottom:8px;color:var(--text-dim);font-size:11px">Generated SQL (${data.model}):</div>
            <pre style="background:var(--bg);padding:10px;border-radius:6px;font-size:12px;overflow-x:auto">${escapeHtml(data.sql)}</pre>
            <div style="margin-top:8px">
                <button class="btn btn-sm btn-success" onclick="runCell(${index})">Run This Query</button>
                <button class="btn btn-sm" onclick="copySql(${index})">Copy SQL</button>
                <button class="btn btn-sm" onclick="insertAsSqlCell(${index})">Insert as SQL Cell</button>
            </div>
        `;
    } catch (err) {
        resultDiv.innerHTML = `<span style="color:var(--red)">${err.message}</span>`;
    }
}

function copySql(index) {
    const cell = currentNotebook.cells[index];
    if (cell._generatedSql) {
        navigator.clipboard.writeText(cell._generatedSql);
    }
}

function insertAsSqlCell(index) {
    const cell = currentNotebook.cells[index];
    if (!cell._generatedSql) return;

    cellCounter++;
    const newCell = {
        id: `c${cellCounter}_${Date.now()}`,
        cell_type: "sql",
        source: cell._generatedSql,
        result: null,
        connection_id: activeConnectionId,
    };

    currentNotebook.cells.splice(index + 1, 0, newCell);
    renderCells();
}

/* ── Chart visualization ──────────────────────────────────── */

function visualizeCell(index) {
    const cell = currentNotebook.cells[index];
    if (!cell.result || !cell.result.columns || cell.result.columns.length < 2) {
        alert("Run the query first. Need at least 2 columns for a chart.");
        return;
    }

    const cols = cell.result.columns;
    document.getElementById("modal-root").innerHTML = `
        <div class="modal-overlay" onclick="if(event.target===this)closeModal()">
            <div class="modal" style="width:600px">
                <h2>Create Chart</h2>
                <div class="form-group">
                    <label>Chart Type</label>
                    <select id="chart-type">
                        <option value="bar">Bar</option>
                        <option value="line">Line</option>
                        <option value="pie">Pie</option>
                        <option value="doughnut">Doughnut</option>
                    </select>
                </div>
                <div class="form-group">
                    <label>X Axis (Labels)</label>
                    <select id="chart-x">${cols.map(c => `<option value="${c}">${c}</option>`).join("")}</select>
                </div>
                <div class="form-group">
                    <label>Y Axis (Values)</label>
                    <select id="chart-y">${cols.map((c, i) => `<option value="${c}" ${i === 1 ? "selected" : ""}>${c}</option>`).join("")}</select>
                </div>
                <div style="height:300px;margin-top:12px"><canvas id="chart-preview"></canvas></div>
                <div class="btn-row">
                    <button class="btn" onclick="closeModal()">Close</button>
                    <button class="btn btn-primary" onclick="renderChartPreview(${index})">Update Chart</button>
                </div>
            </div>
        </div>
    `;

    setTimeout(() => renderChartPreview(index), 100);
}

function renderChartPreview(index) {
    const cell = currentNotebook.cells[index];
    const type = document.getElementById("chart-type").value;
    const xCol = document.getElementById("chart-x").value;
    const yCol = document.getElementById("chart-y").value;

    const labels = cell.result.rows.map(r => String(r[xCol]));
    const values = cell.result.rows.map(r => Number(r[yCol]) || 0);

    const canvas = document.getElementById("chart-preview");
    const existing = Chart.getChart(canvas);
    if (existing) existing.destroy();

    const colors = [
        "#6c5ce7", "#00cec9", "#fdcb6e", "#ff6b6b", "#74b9ff",
        "#a29bfe", "#55efc4", "#ffeaa7", "#fab1a0", "#81ecec",
    ];

    new Chart(canvas, {
        type,
        data: {
            labels: labels.slice(0, 50),
            datasets: [{
                label: yCol,
                data: values.slice(0, 50),
                backgroundColor: type === "pie" || type === "doughnut"
                    ? labels.slice(0, 50).map((_, i) => colors[i % colors.length])
                    : colors[0] + "cc",
                borderColor: colors[0],
                borderWidth: 1,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { labels: { color: "#e4e6f0" } },
            },
            scales: type !== "pie" && type !== "doughnut" ? {
                x: { ticks: { color: "#8b8fa8" }, grid: { color: "#2e3348" } },
                y: { ticks: { color: "#8b8fa8" }, grid: { color: "#2e3348" } },
            } : {},
        },
    });
}

/* ── Markdown toggle ──────────────────────────────────────── */

function toggleMarkdown(index) {
    const cell = currentNotebook.cells[index];
    const editor = document.getElementById(`editor-${cell.id}`);
    const preview = document.getElementById(`preview-${cell.id}`);

    if (preview.style.display === "none") {
        cell.source = editor.value;
        preview.innerHTML = simpleMarkdown(editor.value);
        preview.style.display = "block";
        editor.style.display = "none";
    } else {
        preview.style.display = "none";
        editor.style.display = "block";
    }
}

function simpleMarkdown(text) {
    return text
        .replace(/^### (.+)$/gm, '<h3>$1</h3>')
        .replace(/^## (.+)$/gm, '<h2>$1</h2>')
        .replace(/^# (.+)$/gm, '<h1>$1</h1>')
        .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
        .replace(/\*(.+?)\*/g, '<em>$1</em>')
        .replace(/`(.+?)`/g, '<code>$1</code>')
        .replace(/\n/g, '<br>');
}

/* ── Settings ─────────────────────────────────────────────── */

function showSettingsModal() {
    fetch("/api/settings").then(r => r.json()).then(settings => {
        document.getElementById("modal-root").innerHTML = `
            <div class="modal-overlay" onclick="if(event.target===this)closeModal()">
                <div class="modal">
                    <h2>Settings</h2>
                    <div class="form-group">
                        <label>AI Provider</label>
                        <select id="set-provider">
                            <option value="anthropic" ${settings.ai_provider === "anthropic" ? "selected" : ""}>Anthropic (Claude)</option>
                            <option value="openai" ${settings.ai_provider === "openai" ? "selected" : ""}>OpenAI (GPT)</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label>API Key</label>
                        <input type="password" id="set-apikey" placeholder="sk-...">
                        <div style="font-size:11px;color:var(--text-dim);margin-top:4px">
                            Current: ${settings.ai_api_key || 'not set'}
                        </div>
                    </div>
                    <div class="form-group">
                        <label>Model (optional)</label>
                        <input type="text" id="set-model" value="${settings.ai_model || ''}" placeholder="Leave blank for default">
                    </div>
                    <div class="btn-row">
                        <button class="btn" onclick="closeModal()">Cancel</button>
                        <button class="btn btn-primary" onclick="saveSettings()">Save</button>
                    </div>
                </div>
            </div>
        `;
    });
}

async function saveSettings() {
    const payload = {
        ai_provider: document.getElementById("set-provider").value,
        ai_model: document.getElementById("set-model").value,
    };
    const apiKey = document.getElementById("set-apikey").value;
    if (apiKey) payload.ai_api_key = apiKey;

    await fetch("/api/settings", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
    });
    closeModal();
}

/* ── Utility ──────────────────────────────────────────────── */

function closeModal() {
    document.getElementById("modal-root").innerHTML = "";
}

function escapeHtml(text) {
    const div = document.createElement("div");
    div.textContent = text;
    return div.innerHTML;
}
