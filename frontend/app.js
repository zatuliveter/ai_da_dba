const chatContainer = document.getElementById("chat-container");
const welcomeEl = document.getElementById("welcome");
const dbSelect = document.getElementById("db-select");
const statusDot = document.getElementById("connection-status");
const userInput = document.getElementById("user-input");
const sendBtn = document.getElementById("send-btn");

let ws = null;
let currentDatabase = null;

// ---------------------------------------------------------------------------
// Markdown rendering with highlight.js
// ---------------------------------------------------------------------------

marked.setOptions({
    highlight(code, lang) {
        if (lang && hljs.getLanguage(lang)) {
            return hljs.highlight(code, { language: lang }).value;
        }
        return hljs.highlightAuto(code).value;
    },
    breaks: true,
});

function renderMarkdown(text) {
    return marked.parse(text);
}

// ---------------------------------------------------------------------------
// WebSocket connection
// ---------------------------------------------------------------------------

function connectWS() {
    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    ws = new WebSocket(`${protocol}//${location.host}/ws`);

    ws.onopen = () => {
        setStatus("connected");
        if (currentDatabase) {
            ws.send(JSON.stringify({ type: "set_database", database: currentDatabase }));
        }
    };

    ws.onclose = () => {
        setStatus("disconnected");
        setTimeout(connectWS, 2000);
    };

    ws.onerror = () => setStatus("disconnected");

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        handleMessage(data);
    };
}

function setStatus(state) {
    if (state === "connected") {
        statusDot.className = "w-2.5 h-2.5 rounded-full bg-green-500";
        statusDot.title = "Connected";
    } else {
        statusDot.className = "w-2.5 h-2.5 rounded-full bg-gray-600";
        statusDot.title = "Disconnected";
    }
}

// ---------------------------------------------------------------------------
// Message handling
// ---------------------------------------------------------------------------

function handleMessage(data) {
    switch (data.type) {
        case "answer":
            removeSpinner();
            appendAssistant(data.content);
            setInputEnabled(true);
            break;
        case "tool_call":
            appendToolCall(data.tool, data.args);
            break;
        case "system":
            appendSystem(data.content);
            break;
        case "error":
            removeSpinner();
            appendError(data.content);
            setInputEnabled(true);
            break;
    }
}

// ---------------------------------------------------------------------------
// Chat UI helpers
// ---------------------------------------------------------------------------

function hideWelcome() {
    if (welcomeEl) welcomeEl.style.display = "none";
}

function scrollToBottom() {
    chatContainer.scrollTop = chatContainer.scrollHeight;
}

function appendUser(text) {
    hideWelcome();
    const div = document.createElement("div");
    div.className = "msg-user";
    div.textContent = text;
    chatContainer.appendChild(div);
    scrollToBottom();
}

function appendAssistant(text) {
    hideWelcome();
    const div = document.createElement("div");
    div.className = "msg-assistant";
    div.innerHTML = renderMarkdown(text);
    chatContainer.appendChild(div);
    scrollToBottom();
}

function appendToolCall(tool, args) {
    hideWelcome();

    const argsText = Object.entries(args)
        .map(([k, v]) => `${k}=${typeof v === "string" ? v : JSON.stringify(v)}`)
        .join(", ");

    const div = document.createElement("div");
    div.className = "tool-badge";
    div.innerHTML = `<span class="spinner"></span> ${tool}(${argsText})`;
    chatContainer.appendChild(div);
    scrollToBottom();
}

function appendSystem(text) {
    const div = document.createElement("div");
    div.className = "msg-system";
    div.textContent = text;
    chatContainer.appendChild(div);
    scrollToBottom();
}

function appendError(text) {
    const div = document.createElement("div");
    div.className = "msg-error";
    div.textContent = text;
    chatContainer.appendChild(div);
    scrollToBottom();
}

function showSpinner() {
    const div = document.createElement("div");
    div.id = "thinking-spinner";
    div.className = "tool-badge";
    div.innerHTML = `<span class="spinner"></span> Thinking...`;
    chatContainer.appendChild(div);
    scrollToBottom();
}

function removeSpinner() {
    const el = document.getElementById("thinking-spinner");
    if (el) el.remove();
}

// ---------------------------------------------------------------------------
// Input handling
// ---------------------------------------------------------------------------

function setInputEnabled(enabled) {
    userInput.disabled = !enabled;
    sendBtn.disabled = !enabled || !currentDatabase;
    if (enabled) userInput.focus();
}

function sendMessage() {
    const text = userInput.value.trim();
    if (!text || !ws || ws.readyState !== WebSocket.OPEN || !currentDatabase) return;

    appendUser(text);
    ws.send(JSON.stringify({ type: "message", content: text }));
    userInput.value = "";
    userInput.style.height = "auto";
    setInputEnabled(false);
    showSpinner();
}

sendBtn.addEventListener("click", sendMessage);

userInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        sendMessage();
    }
});

userInput.addEventListener("input", () => {
    userInput.style.height = "auto";
    userInput.style.height = userInput.scrollHeight + "px";
});

// ---------------------------------------------------------------------------
// Database selector
// ---------------------------------------------------------------------------

async function loadDatabases() {
    try {
        const resp = await fetch("/api/databases");
        const data = await resp.json();

        dbSelect.innerHTML = '<option value="">-- select database --</option>';
        for (const db of data.databases) {
            const opt = document.createElement("option");
            opt.value = db;
            opt.textContent = db;
            dbSelect.appendChild(opt);
        }

        if (data.error) {
            appendError("SQL Server connection error: " + data.error);
        }
    } catch (e) {
        dbSelect.innerHTML = '<option value="">Connection error</option>';
        appendError("Failed to connect to backend: " + e.message);
    }
}

dbSelect.addEventListener("change", () => {
    const db = dbSelect.value;
    if (!db) return;

    currentDatabase = db;
    sendBtn.disabled = false;

    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "set_database", database: db }));
    }

    // Clear chat on database switch
    const msgs = chatContainer.querySelectorAll(
        ".msg-user, .msg-assistant, .msg-system, .msg-error, .tool-badge"
    );
    msgs.forEach((m) => m.remove());
    if (welcomeEl) welcomeEl.style.display = "";
});

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

loadDatabases();
connectWS();
