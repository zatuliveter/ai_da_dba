const chatContainer = document.getElementById("chat-container");
const welcomeEl = document.getElementById("welcome");
const dbSelect = document.getElementById("db-select");
const dbDescriptionWrap = document.getElementById("db-description-wrap");
const dbDescription = document.getElementById("db-description");
const chatListEl = document.getElementById("chat-list");
const newChatBtn = document.getElementById("new-chat-btn");
const clearChatBtn = document.getElementById("clear-chat-btn");
const statusDot = document.getElementById("connection-status");
const userInput = document.getElementById("user-input");
const sendBtn = document.getElementById("send-btn");

let ws = null;
let currentDatabase = null;
let currentChatId = null;
let databases = []; // [{name, description}]
let descriptionSaveTimer = null;

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

function highlightCodeBlocks(container) {
    if (typeof hljs === "undefined") return;
    container.querySelectorAll("pre code").forEach((el) => {
        hljs.highlightElement(el);
    });
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
            if (currentChatId != null) {
                ws.send(JSON.stringify({ type: "set_chat", chat_id: currentChatId }));
            }
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
            removeSpinner();
            handleStreamChunk(data.content);
            break;
        case "stream_end":
            removeAllToolBadges();
            currentStreamDiv = null;
            currentStreamContent = "";
            setInputEnabled(true);
            break;
        case "tool_call":
            removeSpinner();
            appendToolCall(data.tool, data.args);
            showSpinner();
            break;
        case "system":
            appendSystem(data.content);
            break;
        case "error":
            removeSpinner();
            removeAllToolBadges();
            appendError(data.content);
            currentStreamDiv = null;
            currentStreamContent = "";
            setInputEnabled(true);
            break;
        case "history_loaded":
            renderHistory(data.messages || []);
            setInputEnabled(true);
            break;
        case "chat_created":
            if (data.chat) {
                addChatToList(data.chat);
                currentChatId = data.chat.id;
                updateChatActiveState();
                clearChatUI();
                saveState();
            }
            break;
        case "chat_cleared":
            clearChatUI();
            setInputEnabled(true);
            break;
    }
}

function renderHistory(messages) {
    clearChatUI();
    if (messages.length === 0) {
        if (welcomeEl) welcomeEl.style.display = "";
        return;
    }
    hideWelcome();
    for (const msg of messages) {
        if (msg.role === "user") {
            appendUser(msg.content);
        } else if (msg.role === "assistant") {
            const div = document.createElement("div");
            div.className = "msg-assistant";
            div.innerHTML = renderMarkdown(msg.content || "");
            highlightCodeBlocks(div);
            chatContainer.appendChild(div);
        }
    }
    scrollToBottom();
}

function clearChatUI() {
    currentStreamDiv = null;
    currentStreamContent = "";
    const msgs = chatContainer.querySelectorAll(
        ".msg-user, .msg-assistant, .msg-system, .msg-error, .tool-badge"
    );
    msgs.forEach((m) => m.remove());
    if (welcomeEl) welcomeEl.style.display = "";
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

/** Remove all tool call badges (and thinking spinner) once the assistant reply is shown. */
function removeAllToolBadges() {
    chatContainer.querySelectorAll(".tool-badge").forEach((el) => el.remove());
}

function handleStreamChunk(content) {
    if (!content) return;
    if (!currentStreamDiv) {
        hideWelcome();
        removeAllToolBadges();
        currentStreamDiv = document.createElement("div");
        currentStreamDiv.className = "msg-assistant";
        chatContainer.appendChild(currentStreamDiv);
    }
    currentStreamContent += content;
    currentStreamDiv.innerHTML = renderMarkdown(currentStreamContent);
    highlightCodeBlocks(currentStreamDiv);
    scrollToBottom();
}
// ---------------------------------------------------------------------------

function setInputEnabled(enabled) {
    userInput.disabled = !enabled;
    const canSend = enabled && currentDatabase && currentChatId != null;
    sendBtn.disabled = !canSend;
    clearChatBtn.disabled = !(currentChatId != null);
    if (enabled) userInput.focus();
}

function sendMessage() {
    const text = userInput.value.trim();
    if (!text) return;

    if (!currentDatabase) {
        appendError("Please select a database first.");
        return;
    }
    if (currentChatId == null) {
        appendError("Please select or create a chat first.");
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
// Database selector & description
// ---------------------------------------------------------------------------

async function loadDatabases() {
    try {
        const resp = await fetch("/api/databases");
        const data = await resp.json();

        databases = data.databases || [];
        dbSelect.innerHTML = '<option value="">-- select database --</option>';
        for (const db of databases) {
            const opt = document.createElement("option");
            opt.value = db.name;
            opt.textContent = db.name;
            dbSelect.appendChild(opt);
        }

        const savedDb = localStorage.getItem("ai_da_dba_db");
        if (savedDb && databases.some((d) => d.name === savedDb)) {
            dbSelect.value = savedDb;
            await onDatabaseChange(savedDb);
        }

        if (data.error) {
            appendError("SQL Server connection error: " + data.error);
        }
    } catch (e) {
        dbSelect.innerHTML = '<option value="">Connection error</option>';
        appendError("Failed to connect to backend: " + e.message);
    }
}

function saveDescription() {
    if (!currentDatabase) return;
    const desc = dbDescription.value.trim();
    fetch(`/api/databases/${encodeURIComponent(currentDatabase)}/description`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ description: desc }),
    }).catch((e) => console.error("Failed to save description:", e));
    // update local cache
    const d = databases.find((x) => x.name === currentDatabase);
    if (d) d.description = desc;
}

dbDescription.addEventListener("input", () => {
    if (descriptionSaveTimer) clearTimeout(descriptionSaveTimer);
    descriptionSaveTimer = setTimeout(saveDescription, 500);
});

dbDescription.addEventListener("blur", () => {
    if (descriptionSaveTimer) {
        clearTimeout(descriptionSaveTimer);
        descriptionSaveTimer = null;
    }
    saveDescription();
});

async function loadChats(dbName) {
    try {
        const resp = await fetch(`/api/databases/${encodeURIComponent(dbName)}/chats`);
        const data = await resp.json();
        const chats = data.chats || [];
        chatListEl.innerHTML = "";
        for (const chat of chats) {
            addChatToList(chat, false);
        }
        updateChatActiveState();
    } catch (e) {
        console.error("Failed to load chats:", e);
        chatListEl.innerHTML = "";
    }
}

function addChatToList(chat, scroll = true) {
    const li = document.createElement("li");
    li.className = "chat-item rounded-lg px-2 py-1.5 text-sm cursor-pointer truncate theme-chat-item";
    li.dataset.chatId = String(chat.id);
    li.title = chat.title || "Chat";
    const title = chat.title || "Новый чат";
    const date = chat.created_at ? new Date(chat.created_at).toLocaleDateString() : "";
    li.textContent = title;
    if (date) {
        const span = document.createElement("span");
        span.className = "block text-xs truncate theme-chat-date";
        span.textContent = date;
        li.appendChild(span);
    }
    li.addEventListener("click", () => selectChat(chat.id));
    chatListEl.appendChild(li);
    if (scroll) chatListEl.scrollTop = 0;
}

function updateChatActiveState() {
    chatListEl.querySelectorAll(".chat-item").forEach((el) => {
        const id = el.dataset.chatId;
        el.classList.toggle("active", id === String(currentChatId));
    });
}

function selectChat(chatId) {
    currentChatId = chatId;
    updateChatActiveState();
    saveState();
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "set_chat", chat_id: chatId }));
    } else {
        connectWS();
    }
}

async function onDatabaseChange(db) {
    currentDatabase = db;
    currentChatId = null;
    updateChatActiveState();
    saveState();

    if (db) {
        dbDescriptionWrap.classList.remove("hidden");
        const d = databases.find((x) => x.name === db);
        dbDescription.value = (d && d.description) || "";
        await loadChats(db);

        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: "set_database", database: db }));
        }

        const savedChat = localStorage.getItem("ai_da_dba_chat");
        if (savedChat) {
            const chatId = parseInt(savedChat, 10);
            const exists = chatListEl.querySelector(`[data-chat-id="${chatId}"]`);
            if (exists) {
                selectChat(chatId);
            }
        }
    } else {
        dbDescriptionWrap.classList.add("hidden");
        dbDescription.value = "";
        chatListEl.innerHTML = "";
    }

    newChatBtn.disabled = !db;
    clearChatUI();
    setInputEnabled(true);
}

dbSelect.addEventListener("change", () => {
    const db = dbSelect.value;
    onDatabaseChange(db || null);
});

newChatBtn.addEventListener("click", () => {
    if (!currentDatabase) return;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        appendError("WebSocket is not connected.");
        connectWS();
        return;
    }
    ws.send(JSON.stringify({ type: "create_chat", title: "Новый чат" }));
});

clearChatBtn.addEventListener("click", () => {
    if (currentChatId == null) return;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    ws.send(JSON.stringify({ type: "clear_chat" }));
});

function saveState() {
    if (currentDatabase) localStorage.setItem("ai_da_dba_db", currentDatabase);
    if (currentChatId != null) localStorage.setItem("ai_da_dba_chat", String(currentChatId));
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

loadDatabases();
connectWS();
