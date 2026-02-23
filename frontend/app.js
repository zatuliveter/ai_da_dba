const chatContainer = document.getElementById("chat-container");
const welcomeEl = document.getElementById("welcome");
const dbSelect = document.getElementById("db-select");
const statusDot = document.getElementById("connection-status");
const userInput = document.getElementById("user-input");
const sendBtn = document.getElementById("send-btn");

let ws = null;
let currentDatabase = null;

// stream state
let currentStreamDiv = null;
let currentStreamContent = "";

// ---------------------------------------------------------------------------
// Theme toggle
// ---------------------------------------------------------------------------

const themeToggle = document.getElementById("theme-toggle");
const iconSun = document.getElementById("icon-sun");
const iconMoon = document.getElementById("icon-moon");
const hljsLink = document.getElementById("hljs-theme");

function setTheme(theme) {
    document.documentElement.setAttribute("data-theme", theme);
    localStorage.setItem("theme", theme);

    const isLight = theme === "light";
    iconSun.classList.toggle("hidden", isLight);
    iconMoon.classList.toggle("hidden", !isLight);
    hljsLink.href = isLight
        ? "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github.min.css"
        : "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github-dark.min.css";
}

themeToggle.addEventListener("click", () => {
    const current = document.documentElement.getAttribute("data-theme") || "dark";
    setTheme(current === "dark" ? "light" : "dark");
});

setTheme(localStorage.getItem("theme") || "dark");

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
        console.log("[WS] Connected");
        setStatus("connected");
        if (currentDatabase) {
            ws.send(JSON.stringify({ type: "set_database", database: currentDatabase }));
        }
    };

    ws.onclose = (e) => {
        console.log("[WS] Closed:", e.code, e.reason);
        setStatus("disconnected");
        setTimeout(connectWS, 2000);
    };

    ws.onerror = (e) => {
        console.error("[WS] Error:", e);
        setStatus("disconnected");
    };

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        console.log("[WS] Message:", data.type, data);
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
        case "stream":
            removeSpinner(); // remove thinking spinner when text starts flowing
            handleStreamChunk(data.content);
            break;
        case "stream_end":
            currentStreamDiv = null;
            currentStreamContent = "";
            setInputEnabled(true);
            break;
        case "tool_call":
            removeSpinner(); // remove previous spinner
            appendToolCall(data.tool, data.args);
            showSpinner(); // show spinner again while tool is executing
            break;
        case "system":
            appendSystem(data.content);
            break;
        case "error":
            removeSpinner();
            appendError(data.content);
            currentStreamDiv = null;
            currentStreamContent = "";
            setInputEnabled(true);
            break;
    }
}

function handleStreamChunk(textChunk) {
    hideWelcome();
    
    // create the message container on the first chunk
    if (!currentStreamDiv) {
        currentStreamDiv = document.createElement("div");
        currentStreamDiv.className = "msg-assistant";
        chatContainer.appendChild(currentStreamDiv);
    }

    // append new raw text to the accumulator
    currentStreamContent += textChunk;
    
    // re-render the entire accumulated string as markdown
    currentStreamDiv.innerHTML = renderMarkdown(currentStreamContent);
    
    scrollToBottom();
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
    if (!text) return;

    if (!currentDatabase) {
        appendError("Please select a database first.");
        return;
    }
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        appendError("WebSocket is not connected. Reconnecting...");
        connectWS();
        return;
    }

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
    currentStreamDiv = null;
    currentStreamContent = "";
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
