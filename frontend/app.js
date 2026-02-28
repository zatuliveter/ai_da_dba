const chatContainer = document.getElementById("chat-container");
const chatInner = document.getElementById("chat-inner");
const welcomeEl = document.getElementById("welcome");
const dbSelect = document.getElementById("db-select");
const dbDescriptionWrap = document.getElementById("db-description-wrap");
const dbDescription = document.getElementById("db-description");
const chatListEl = document.getElementById("chat-list");
const newChatBtn = document.getElementById("new-chat-btn");
const statusDot = document.getElementById("connection-status");
const userInput = document.getElementById("user-input");
const sendBtn = document.getElementById("send-btn");
const roleSelect = document.getElementById("role-select");
const fileInput = document.getElementById("file-input");
const attachBtn = document.getElementById("attach-btn");
const attachmentsListWrap = document.getElementById("attachments-list-wrap");

let ws = null;
let currentDatabase = null;
let currentChatId = null;
let databases = []; // [{name, description}]
let descriptionSaveTimer = null;
let pendingMessage = null;
let pendingAttachments = null; // File[] when creating new chat
let attachedFiles = []; // File[] for current compose
let currentRole = localStorage.getItem("ai_da_dba_role") || "assistant";

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
        ws.send(JSON.stringify({ type: "set_role", role: currentRole }));
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
            markAllToolBadgesDone();
            currentStreamDiv = null;
            currentStreamContent = "";
            setInputEnabled(true);
            break;
        case "tool_call":
            removeSpinner();
            appendToolCall(data.tool, data.args);
            showSpinner();
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
                addChatToList(data.chat, true);
                currentChatId = data.chat.id;
                updateChatActiveState();
                clearChatUI();
                saveState();
                if (pendingMessage != null || (pendingAttachments && pendingAttachments.length)) {
                    const text = pendingMessage || "";
                    (async () => {
                        let savedFilenames = [];
                        if (pendingAttachments && pendingAttachments.length) {
                            try {
                                const form = new FormData();
                                for (const f of pendingAttachments) {
                                    form.append("files", f);
                                }
                                const url = `/api/databases/${encodeURIComponent(currentDatabase)}/chats/${currentChatId}/files`;
                                const resp = await fetch(url, { method: "POST", body: form });
                                if (!resp.ok) throw new Error("Upload failed");
                                const uploadData = await resp.json();
                                if (uploadData.errors && uploadData.errors.length) throw new Error(uploadData.errors[0]);
                                savedFilenames = (uploadData.uploaded || []).map((u) => u.saved_as);
                            } catch (e) {
                                appendError("Upload failed: " + e.message);
                                setInputEnabled(true);
                                pendingMessage = null;
                                pendingAttachments = null;
                                return;
                            }
                        }
                        appendUser(text || "(attachments only)", savedFilenames);
                        ws.send(JSON.stringify({
                            type: "message",
                            content: text,
                            attachments: savedFilenames,
                        }));
                        pendingMessage = null;
                        pendingAttachments = null;
                        setInputEnabled(false);
                        showSpinner();
                    })();
                }
            }
            break;
    }
}

function renderHistory(messages) {
    if (messages.length === 0) {
        const hasContent =
            chatContainer.querySelector(".msg-user, .msg-assistant, .msg-system, .msg-error, .tool-badge") ||
            document.getElementById("thinking-spinner") ||
            currentStreamDiv;
        if (hasContent) return;
        clearChatUI();
        if (welcomeEl) welcomeEl.style.display = "";
        return;
    }
    clearChatUI();
    hideWelcome();
    for (const msg of messages) {
        if (msg.role === "user") {
            const { attachmentNames, userText } = parseUserContent(msg.content);
            appendUser(userText, attachmentNames.length ? attachmentNames : undefined);
        } else if (msg.role === "assistant") {
            const text = (msg.content || "").trim();
            if (text) {
                const div = document.createElement("div");
                div.className = "msg-assistant";
                div.innerHTML = renderMarkdown(text);
                highlightCodeBlocks(div);
                chatInner.appendChild(div);
            }
        } else if (msg.role === "tool_call") {
            appendToolCallDone(msg.content || "");
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

const SCROLL_BOTTOM_THRESHOLD = 80;

function scrollToBottom() {
    chatContainer.scrollTop = chatContainer.scrollHeight;
}

function isUserNearBottom() {
    const { scrollTop, clientHeight, scrollHeight } = chatContainer;
    return scrollTop + clientHeight >= scrollHeight - SCROLL_BOTTOM_THRESHOLD;
}

/** Parse user message content that may include "Attached file: name\n\n...\n\n---\n\n" blocks. */
function parseUserContent(content) {
    if (!content || !content.includes("\n\n---\n\n")) {
        return { attachmentNames: [], userText: content || "" };
    }
    const parts = content.split("\n\n---\n\n");
    const userText = (parts.length ? parts[parts.length - 1] : "").trim();
    const attachmentNames = [];
    for (let i = 0; i < parts.length - 1; i++) {
        const m = parts[i].match(/^Attached file: ([^\n]+)/);
        if (m) attachmentNames.push(m[1].trim());
    }
    return { attachmentNames, userText };
}

function appendUser(text, attachmentFilenames) {
    hideWelcome();
    const div = document.createElement("div");
    div.className = "msg-user";
    if (attachmentFilenames && attachmentFilenames.length) {
        const linksWrap = document.createElement("div");
        linksWrap.className = "msg-user-attachments";
        for (const name of attachmentFilenames) {
            const chip = document.createElement("a");
            chip.href = `/api/databases/${encodeURIComponent(currentDatabase)}/chats/${currentChatId}/files/${encodeURIComponent(name)}`;
            chip.target = "_blank";
            chip.rel = "noopener";
            chip.className = "msg-user-file";
            chip.innerHTML = `${FILE_ICON_SVG}<span class="msg-user-file-name">${escapeHtml(name)}</span>`;
            linksWrap.appendChild(chip);
        }
        div.appendChild(linksWrap);
    }
    const textNode = document.createElement("div");
    textNode.className = "msg-user-text";
    textNode.textContent = text || "";
    div.appendChild(textNode);
    chatInner.appendChild(div);
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
    chatInner.appendChild(div);
    scrollToBottom();
}

/** Append a tool call badge in "done" state (e.g. when loading history). */
function appendToolCallDone(content) {
    hideWelcome();
    const div = document.createElement("div");
    div.className = "tool-badge tool-badge--done";
    div.innerHTML = `<span class="tool-done" aria-hidden="true">✓</span> ${content}`;
    chatInner.appendChild(div);
    scrollToBottom();
}


function appendError(text) {
    const div = document.createElement("div");
    div.className = "msg-error";
    div.textContent = text;
    chatInner.appendChild(div);
    scrollToBottom();
}

function showSpinner() {
    const div = document.createElement("div");
    div.id = "thinking-spinner";
    div.className = "tool-badge";
    div.innerHTML = `<span class="spinner"></span> Thinking...`;
    chatInner.appendChild(div);
    scrollToBottom();
}

function removeSpinner() {
    const el = document.getElementById("thinking-spinner");
    if (el) el.remove();
}

/** Remove all tool call badges (and thinking spinner). Used on error. */
function removeAllToolBadges() {
    chatContainer.querySelectorAll(".tool-badge").forEach((el) => el.remove());
}

/** On stream_end: remove Thinking spinner, mark each tool badge as done (stop spinner, show checkmark). */
function markAllToolBadgesDone() {
    const spinnerEl = document.getElementById("thinking-spinner");
    if (spinnerEl) spinnerEl.remove();
    chatContainer.querySelectorAll(".tool-badge").forEach((el) => {
        const spinner = el.querySelector(".spinner");
        if (spinner) {
            spinner.remove();
            const check = document.createElement("span");
            check.className = "tool-done";
            check.innerHTML = "✓";
            check.setAttribute("aria-hidden", "true");
            el.insertBefore(check, el.firstChild);
        }
        el.classList.add("tool-badge--done");
    });
}

function handleStreamChunk(content) {
    if (!content) return;
    if (!currentStreamDiv) {
        hideWelcome();
        currentStreamDiv = document.createElement("div");
        currentStreamDiv.className = "msg-assistant";
        chatInner.appendChild(currentStreamDiv);
    }
    currentStreamContent += content;
    currentStreamDiv.innerHTML = renderMarkdown(currentStreamContent);
    highlightCodeBlocks(currentStreamDiv);
    if (isUserNearBottom()) scrollToBottom();
}
// ---------------------------------------------------------------------------

function setInputEnabled(enabled) {
    userInput.disabled = !enabled;
    const canSend = enabled && currentDatabase;
    sendBtn.disabled = !canSend;
    if (enabled) userInput.focus();
}

async function uploadFilesForChat(chatId) {
    if (!attachedFiles.length) return [];
    const form = new FormData();
    for (const f of attachedFiles) {
        form.append("files", f);
    }
    const url = `/api/databases/${encodeURIComponent(currentDatabase)}/chats/${chatId}/files`;
    const resp = await fetch(url, { method: "POST", body: form });
    if (!resp.ok) {
        const err = await resp.json().catch(() => ({}));
        throw new Error(err.detail || err.error || `Upload failed: ${resp.status}`);
    }
    const data = await resp.json();
    if (data.errors && data.errors.length) {
        throw new Error(data.errors.join("; "));
    }
    return (data.uploaded || []).map((u) => u.saved_as);
}

function sendMessage() {
    const text = userInput.value.trim();
    const hasAttachments = attachedFiles.length > 0;
    if (!text && !hasAttachments) return;

    if (!currentDatabase) {
        appendError("Please select a database first.");
        return;
    }
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        appendError("WebSocket is not connected. Reconnecting...");
        connectWS();
        return;
    }

    if (currentChatId == null) {
        pendingMessage = text;
        pendingAttachments = attachedFiles.length ? [...attachedFiles] : null;
        userInput.value = "";
        attachedFiles = [];
        renderAttachmentsList();
        userInput.style.height = "auto";
        const title = text.split("\n")[0].trim().slice(0, 40) || "Новый чат";
        ws.send(JSON.stringify({ type: "create_chat", title }));
        setInputEnabled(false);
        return;
    }

    (async () => {
        let savedFilenames = [];
        if (hasAttachments) {
            try {
                savedFilenames = await uploadFilesForChat(currentChatId);
            } catch (e) {
                appendError("Upload failed: " + e.message);
                setInputEnabled(true);
                return;
            }
        }
        const displayText = text || "(attachments only)";
        appendUser(displayText, savedFilenames);
        ws.send(JSON.stringify({
            type: "message",
            content: text,
            attachments: savedFilenames,
        }));
        userInput.value = "";
        attachedFiles = [];
        renderAttachmentsList();
        userInput.style.height = "auto";
        setInputEnabled(false);
        showSpinner();
    })();
}

const validRoles = ["assistant", "dba"];
if (!validRoles.includes(currentRole)) currentRole = "assistant";
roleSelect.value = currentRole;
roleSelect.addEventListener("change", () => {
    currentRole = roleSelect.value || "assistant";
    localStorage.setItem("ai_da_dba_role", currentRole);
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "set_role", role: currentRole }));
    }
});

// ---------------------------------------------------------------------------
// File attachments
// ---------------------------------------------------------------------------

const FILE_ICON_SVG = `<svg class="attachment-chip-icon" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/></svg>`;

function renderAttachmentsList() {
    attachmentsListWrap.innerHTML = "";
    if (attachedFiles.length === 0) {
        attachmentsListWrap.classList.add("hidden");
        return;
    }
    attachmentsListWrap.classList.remove("hidden");
    for (let i = 0; i < attachedFiles.length; i++) {
        const file = attachedFiles[i];
        const chip = document.createElement("span");
        chip.className = "attachment-chip";
        chip.innerHTML = `<span class="attachment-chip-left">${FILE_ICON_SVG}<span class="attachment-chip-name">${escapeHtml(file.name)}</span></span><button type="button" class="attachment-chip-remove" data-index="${i}" aria-label="Remove">×</button>`;
        const removeBtn = chip.querySelector(".attachment-chip-remove");
        removeBtn.addEventListener("click", () => {
            attachedFiles.splice(i, 1);
            renderAttachmentsList();
        });
        attachmentsListWrap.appendChild(chip);
    }
}

function escapeHtml(s) {
    const div = document.createElement("div");
    div.textContent = s;
    return div.innerHTML;
}


fileInput.addEventListener("change", () => {
    const list = Array.from(fileInput.files || []);
    const allowed = [".txt", ".sql", ".xml", ".json", ".md", ".csv", ".xdl", ".sqlplan"];
    const isText = (f) => allowed.some((ext) => f.name.toLowerCase().endsWith(ext)) || (f.type && f.type.startsWith("text/"));
    for (const f of list) {
        if (isText(f) && !attachedFiles.some((a) => a.name === f.name && a.size === f.size)) {
            attachedFiles.push(f);
        }
    }
    fileInput.value = "";
    renderAttachmentsList();
});

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
        const chats = (data.chats || []).slice();
        // Backend returns starred first, then by date; keep that order
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

function addChatToList(chat, insertAtTop = false) {
    const starred = !!chat.starred;
    const li = document.createElement("li");
    li.className = "chat-item rounded-lg px-2 py-1.5 text-sm cursor-pointer theme-chat-item";
    li.dataset.chatId = String(chat.id);
    li.title = chat.title || "Chat";

    const starBtn = document.createElement("button");
    starBtn.type = "button";
    starBtn.className = "chat-star-btn flex-shrink-0 p-0.5 rounded hover:opacity-100";
    starBtn.title = starred ? "Unstar" : "Star";
    starBtn.setAttribute("aria-label", starred ? "Unstar" : "Star");
    starBtn.innerHTML = starred
        ? "<svg class=\"w-4 h-4 theme-chat-star-filled\" fill=\"currentColor\" viewBox=\"0 0 24 24\"><path d=\"M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z\"/></svg>"
        : "<svg class=\"w-4 h-4 theme-chat-star\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" viewBox=\"0 0 24 24\"><path d=\"M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z\"/></svg>";
    starBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        if (!currentDatabase) return;
        const newStarred = !starred;
        fetch(`/api/databases/${encodeURIComponent(currentDatabase)}/chats/${chat.id}/star`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ starred: newStarred }),
        })
            .then((res) => res.json())
            .then(() => loadChats(currentDatabase))
            .catch((err) => console.error("Failed to toggle star:", err));
    });

    const titleText = chat.title || "Новый чат";
    const titleSpan = document.createElement("span");
    titleSpan.className = "chat-item-title flex-1 min-w-0 truncate";
    titleSpan.textContent = titleText;    
    titleSpan.addEventListener("dblclick", (e) => {
        e.stopPropagation();
        e.preventDefault();
        setTimeout(() => startEditChatTitle(li, chat.id, titleSpan), 0);
    });

    const row1 = document.createElement("div");
    row1.className = "chat-item-row";
    row1.appendChild(starBtn);
    row1.appendChild(titleSpan);

    const dateTime = chat.created_at
        ? new Date(chat.created_at).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" })
        : "";
    const dateSpan = document.createElement("span");
    dateSpan.className = "chat-item-date text-xs truncate theme-chat-date";
    dateSpan.textContent = dateTime;

    const editBtn = document.createElement("button");
    editBtn.type = "button";
    editBtn.className = "chat-edit-btn flex-shrink-0 p-0.5 rounded opacity-0 theme-chat-edit";
    editBtn.title = "Edit title";
    editBtn.setAttribute("aria-label", "Edit title");
    editBtn.innerHTML = "<svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" d=\"M15.232 5.232l3.536 3.536m-2.036-5.036a2.5 2.5 0 113.536 3.536L6.5 21.036H3v-3.572L16.732 3.732z\"/></svg>";
    editBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        startEditChatTitle(li, chat.id, titleSpan);
    });

    const deleteBtn = document.createElement("button");
    deleteBtn.type = "button";
    deleteBtn.className = "chat-delete-btn flex-shrink-0 p-0.5 rounded opacity-0 theme-chat-delete";
    deleteBtn.title = "Delete chat";
    deleteBtn.setAttribute("aria-label", "Delete chat");
    deleteBtn.innerHTML = "<svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" d=\"M6 18L18 6M6 6l12 12\"/></svg>";
    deleteBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        if (!currentDatabase) return;
        if (!confirm("Удалить этот чат? Действие нельзя отменить.")) return;
        fetch(`/api/databases/${encodeURIComponent(currentDatabase)}/chats/${chat.id}`, {
            method: "DELETE",
        })
            .then((res) => {
                if (!res.ok) throw new Error("Delete failed");
                return res.json();
            })
            .then(() => {
                li.remove();
                if (currentChatId === chat.id) {
                    currentChatId = null;
                    clearChatUI();
                    updateChatActiveState();
                    saveState();
                    if (ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({ type: "set_database", database: currentDatabase }));
                    }
                }
            })
            .catch((err) => console.error("Failed to delete chat:", err));
    });

    const row2 = document.createElement("div");
    row2.className = "chat-item-meta";
    row2.appendChild(dateSpan);
    row2.appendChild(editBtn);
    row2.appendChild(deleteBtn);

    li.appendChild(row1);
    li.appendChild(row2);

    li.addEventListener("click", (e) => {
        if (e.target.closest(".chat-star-btn") || e.target.closest(".chat-delete-btn") || e.target.closest(".chat-edit-btn") || e.target.closest("input.chat-item-title-input")) return;
        selectChat(chat.id);
    });

    if (insertAtTop && chatListEl.firstChild) {
        chatListEl.insertBefore(li, chatListEl.firstChild);
    } else {
        chatListEl.appendChild(li);
    }
    if (insertAtTop) chatListEl.scrollTop = 0;
}

function startEditChatTitle(li, chatId, titleSpan) {
    if (!currentDatabase) return;
    const currentTitle = titleSpan.textContent || "Новый чат";
    const input = document.createElement("input");
    input.type = "text";
    input.className = "chat-item-title-input flex-1 min-w-0 text-inherit bg-transparent border border-solid theme-input rounded px-1 py-0 text-sm";
    input.value = currentTitle;
    input.maxLength = 80;
    input.setAttribute("aria-label", "Chat title");

    const row1 = li.querySelector(".chat-item-row");
    row1.replaceChild(input, titleSpan);
    input.focus();
    input.select();

    const openedAt = Date.now();
    const BLUR_GRACE_MS = 200;

    function finishEdit(save) {
        if (Date.now() - openedAt < BLUR_GRACE_MS) return;
        const newTitle = (input.value.trim() || "Новый чат").slice(0, 80);
        row1.replaceChild(titleSpan, input);
        if (save && newTitle !== currentTitle) {
            fetch(`/api/databases/${encodeURIComponent(currentDatabase)}/chats/${chatId}/title`, {
                method: "PATCH",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ title: newTitle }),
            })
                .then((res) => res.json())
                .then((data) => {
                    titleSpan.textContent = (data && data.title) || newTitle;
                    li.title = titleSpan.textContent;
                })
                .catch((err) => console.error("Failed to update chat title:", err));
        } else if (save) {
            titleSpan.textContent = newTitle;
            li.title = titleSpan.textContent;
        }
    }

    input.addEventListener("blur", () => finishEdit(true));
    input.addEventListener("keydown", (e) => {
        e.stopPropagation();
        if (e.key === "Enter") {
            e.preventDefault();
            finishEdit(true);
        } else if (e.key === "Escape") {
            e.preventDefault();
            finishEdit(false);
        }
    });
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
        } else if (!ws || ws.readyState === WebSocket.CLOSED) {
            connectWS();
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
    currentChatId = null;
    updateChatActiveState();
    clearChatUI();
    saveState();
    setInputEnabled(true);
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "set_database", database: currentDatabase }));
    }
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
document.getElementById("app-title").addEventListener("click", () => location.reload());
