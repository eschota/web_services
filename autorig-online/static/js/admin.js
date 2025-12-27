/**
 * AutoRig Online - Admin Panel
 */

const Admin = {
    view: 'users', // 'users' | 'anon' | 'tasks'
    currentPage: 1,
    perPage: 20,
    totalItems: 0,
    sortBy: 'created_at',
    sortDesc: true,
    searchQuery: '',
    selectedUser: null,
    tasksCurrentPage: 1,
    tasksPerPage: 20,
    tasksTotal: 0,
    selectedTasksOwner: null, // { type: 'user'|'anon', id: string|number, label: string }
    tasksStatusFilter: '', // '' = all, 'processing', 'created', 'done', 'error'
    statusCounts: { processing: 0, created: 0, done: 0, error: 0 },
    
    async init() {
        // Init i18n
        await I18n.init();
        
        // Setup theme
        this.setupTheme();
        
        // Default: tasks view (sorted by date, newest first)
        this.applyView('tasks');
        
        // Setup event listeners
        this.setupEventListeners();
    },
    
    setupTheme() {
        const toggle = document.getElementById('theme-toggle');
        const savedTheme = localStorage.getItem('autorig_theme') || 'dark';
        document.documentElement.setAttribute('data-theme', savedTheme);
        toggle.textContent = savedTheme === 'dark' ? '☀️' : '🌙';
        
        toggle.addEventListener('click', () => {
            const current = document.documentElement.getAttribute('data-theme');
            const newTheme = current === 'dark' ? 'light' : 'dark';
            document.documentElement.setAttribute('data-theme', newTheme);
            localStorage.setItem('autorig_theme', newTheme);
            toggle.textContent = newTheme === 'dark' ? '☀️' : '🌙';
        });
    },
    
    setupEventListeners() {
        // Restart service
        const restartBtn = document.getElementById('restart-service-btn');
        restartBtn?.addEventListener('click', async () => {
            const ok = confirm('Restart backend service now?');
            if (!ok) return;

            restartBtn.disabled = true;
            restartBtn.textContent = 'Restarting...';
            try {
                const resp = await fetch('/api/admin/service/restart', { method: 'POST' });
                const data = await resp.json();
                if (!resp.ok) {
                    alert(data.detail || 'Failed to restart service');
                    restartBtn.disabled = false;
                    restartBtn.textContent = 'Restart service';
                    return;
                }

                alert('Service restart scheduled. Page will reload in ~5 seconds.');
                setTimeout(() => window.location.reload(), 5000);
            } catch (e) {
                console.error('Service restart error:', e);
                alert('Failed to restart service');
                restartBtn.disabled = false;
                restartBtn.textContent = 'Restart service';
            }
        });

        // Search
        const searchInput = document.getElementById('search-input');
        let searchTimeout;
        searchInput.addEventListener('input', () => {
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(() => {
                this.searchQuery = searchInput.value;
                this.currentPage = 1;
                this.loadList();
            }, 300);
        });
        
        // Sort headers (delegated; headers are dynamic per view)
        document.getElementById('admin-table-head')?.addEventListener('click', (e) => {
            const th = e.target.closest('[data-sort]');
            if (!th) return;
                const field = th.getAttribute('data-sort');
            if (!field) return;
                if (this.sortBy === field) {
                    this.sortDesc = !this.sortDesc;
                } else {
                    this.sortBy = field;
                    this.sortDesc = true;
                }
            this.loadList();
        });
        
        // Pagination
        document.getElementById('prev-page').addEventListener('click', () => {
            if (this.currentPage > 1) {
                this.currentPage--;
                this.loadList();
            }
        });
        
        document.getElementById('next-page').addEventListener('click', () => {
            const totalPages = Math.ceil(this.totalItems / this.perPage);
            if (this.currentPage < totalPages) {
                this.currentPage++;
                this.loadList();
            }
        });
        
        // Modal
        document.getElementById('modal-cancel').addEventListener('click', () => this.closeModal());
        document.getElementById('modal-save').addEventListener('click', () => this.saveBalance());
        
        // Close modal on backdrop click
        document.getElementById('balance-modal').addEventListener('click', (e) => {
            if (e.target.id === 'balance-modal') {
                this.closeModal();
            }
        });
        
        // Tasks modal
        document.getElementById('tasks-modal-close').addEventListener('click', () => this.closeTasksModal());
        document.getElementById('tasks-modal').addEventListener('click', (e) => {
            if (e.target.id === 'tasks-modal') {
                this.closeTasksModal();
            }
        });
        
        // Tasks pagination
        document.getElementById('tasks-prev-page').addEventListener('click', () => {
            if (this.tasksCurrentPage > 1) {
                this.tasksCurrentPage--;
                this.loadOwnerTasks();
            }
        });
        
        document.getElementById('tasks-next-page').addEventListener('click', () => {
            const totalPages = Math.ceil(this.tasksTotal / this.tasksPerPage);
            if (this.tasksCurrentPage < totalPages) {
                this.tasksCurrentPage++;
                this.loadOwnerTasks();
            }
        });

        // Tabs
        document.getElementById('tab-users')?.addEventListener('click', () => this.applyView('users'));
        document.getElementById('tab-anon')?.addEventListener('click', () => this.applyView('anon'));
        document.getElementById('tab-tasks')?.addEventListener('click', () => this.applyView('tasks'));

        // Status filter (for tasks view)
        document.getElementById('status-filter')?.addEventListener('change', (e) => {
            this.tasksStatusFilter = e.target.value;
            this.currentPage = 1;
            this.loadList();
        });
    },

    applyView(view) {
        this.view = view;
        this.currentPage = 1;
        this.sortDesc = true;
        this.searchQuery = '';
        const searchInput = document.getElementById('search-input');
        if (searchInput) searchInput.value = '';

        const statusFilter = document.getElementById('status-filter');
        const title = document.getElementById('admin-table-title');
        const ul = document.getElementById('stat-users-label');
        const tl = document.getElementById('stat-tasks-label');
        const cl = document.getElementById('stat-credits-label');

        if (view === 'users') {
            this.sortBy = 'created_at';
            if (title) title.textContent = 'Users';
            if (searchInput) searchInput.placeholder = 'Search by email...';
            if (ul) ul.textContent = 'Total Users';
            if (tl) tl.textContent = 'Total Tasks';
            if (cl) cl.textContent = 'Total Credits';
            if (statusFilter) statusFilter.classList.add('hidden');
        } else if (view === 'anon') {
            this.sortBy = 'last_seen_at';
            if (title) title.textContent = 'Anon Sessions';
            if (searchInput) searchInput.placeholder = 'Search by anon id...';
            if (ul) ul.textContent = 'Total Anon Sessions';
            if (tl) tl.textContent = 'Tasks (page)';
            if (cl) cl.textContent = '-';
            if (statusFilter) statusFilter.classList.add('hidden');
        } else if (view === 'tasks') {
            this.sortBy = 'created_at';
            this.tasksStatusFilter = '';
            if (statusFilter) {
                statusFilter.classList.remove('hidden');
                statusFilter.value = '';
            }
            if (title) title.textContent = 'All Tasks';
            if (searchInput) searchInput.placeholder = 'Search...';
            if (ul) ul.textContent = 'Processing';
            if (tl) tl.textContent = 'Queue';
            if (cl) cl.textContent = 'Done';
        }

        // active styles
        const btnUsers = document.getElementById('tab-users');
        const btnAnon = document.getElementById('tab-anon');
        const btnTasks = document.getElementById('tab-tasks');
        [btnUsers, btnAnon, btnTasks].forEach(btn => {
            if (!btn) return;
            const isActive = (btn.id === 'tab-users' && view === 'users') ||
                             (btn.id === 'tab-anon' && view === 'anon') ||
                             (btn.id === 'tab-tasks' && view === 'tasks');
            btn.classList.toggle('btn-primary', isActive);
            btn.classList.toggle('btn-secondary', !isActive);
        });

        this.renderListHeader();
        this.loadList();
    },

    renderListHeader() {
        const head = document.getElementById('admin-table-head');
        if (!head) return;
        if (this.view === 'users') {
            head.innerHTML = `
                <th data-sort="id">ID</th>
                <th data-sort="email">Email</th>
                <th data-sort="name">Name</th>
                <th data-sort="balance_credits">Balance</th>
                <th data-sort="total_tasks">Tasks</th>
                <th data-sort="last_login_at">Last Login</th>
                <th>Actions</th>
            `;
        } else if (this.view === 'anon') {
            head.innerHTML = `
                <th data-sort="anon_id">Anon ID</th>
                <th data-sort="free_used">Free Used</th>
                <th data-sort="total_tasks">Tasks</th>
                <th data-sort="created_at">Created</th>
                <th data-sort="last_seen_at">Last Seen</th>
                <th>Actions</th>
            `;
        } else if (this.view === 'tasks') {
            head.innerHTML = `
                <th style="width:60px;">Preview</th>
                <th data-sort="id">Task ID</th>
                <th>Owner</th>
                <th data-sort="status">Status</th>
                <th>Progress</th>
                <th>Worker</th>
                <th data-sort="created_at">Created</th>
                <th>Actions</th>
            `;
        }
    },

    async loadList() {
        if (this.view === 'users') return await this.loadUsers();
        if (this.view === 'anon') return await this.loadAnonSessions();
        if (this.view === 'tasks') return await this.loadAllTasks();
    },
    
    async loadUsers() {
        try {
            const params = new URLSearchParams({
                page: this.currentPage,
                per_page: this.perPage,
                sort_by: this.sortBy,
                sort_desc: this.sortDesc
            });
            
            if (this.searchQuery) {
                params.append('query', this.searchQuery);
            }
            
            const response = await fetch(`/api/admin/users?${params}`);
            
            if (response.status === 403) {
                window.location.href = '/';
                return;
            }
            
            if (!response.ok) {
                throw new Error('Failed to load users');
            }
            
            const data = await response.json();
            this.totalItems = data.total;
            
            this.renderUsers(data.users);
            this.updatePagination();
            this.updateStatsUsers(data);
        } catch (error) {
            console.error('Error loading users:', error);
        }
    },
    
    renderUsers(users) {
        const tbody = document.getElementById('admin-table-body');
        
        if (users.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="7" style="text-align: center; color: var(--text-muted);">No users found</td>
                </tr>
            `;
            return;
        }
        
        tbody.innerHTML = users.map(user => `
            <tr style="cursor: pointer;" data-user-id="${user.id}" data-user-email="${user.email}">
                <td>${user.id}</td>
                <td>${user.email}</td>
                <td>${user.name || '-'}</td>
                <td>
                    <span style="color: var(--accent); font-weight: 600;">${user.balance_credits}</span>
                </td>
                <td>${user.total_tasks}</td>
                <td>${this.formatDate(user.last_login_at)}</td>
                <td>
                    <button class="btn btn-secondary" style="padding: 0.25rem 0.5rem; font-size: 0.75rem;" 
                            onclick="event.stopPropagation(); Admin.openBalanceModal(${user.id}, '${user.email}', ${user.balance_credits})">
                        Edit Balance
                    </button>
                </td>
            </tr>
        `).join('');
        
        // Add click handlers to rows
        tbody.querySelectorAll('tr[data-user-id]').forEach(row => {
            row.addEventListener('click', (e) => {
                // Don't open tasks modal if clicking on button
                if (e.target.closest('button')) return;
                
                const userId = parseInt(row.getAttribute('data-user-id'));
                const userEmail = row.getAttribute('data-user-email');
                this.openUserTasksModal(userId, userEmail);
            });
        });
    },
    
    updatePagination() {
        const totalPages = Math.ceil(this.totalItems / this.perPage);
        const start = this.totalItems > 0 ? (this.currentPage - 1) * this.perPage + 1 : 0;
        const end = Math.min(this.currentPage * this.perPage, this.totalItems);

        let label = 'items';
        if (this.view === 'users') label = 'users';
        else if (this.view === 'anon') label = 'anon sessions';
        else if (this.view === 'tasks') label = 'tasks';
        
        document.getElementById('pagination-info').textContent = `Showing ${start}-${end} of ${this.totalItems} ${label}`;
        
        document.getElementById('prev-page').disabled = this.currentPage <= 1;
        document.getElementById('next-page').disabled = this.currentPage >= totalPages;
    },
    
    updateStatsUsers(data) {
        document.getElementById('stat-users').textContent = data.total;
        let totalTasks = 0;
        let totalCredits = 0;
        data.users.forEach(u => {
            totalTasks += u.total_tasks;
            totalCredits += u.balance_credits;
        });
        document.getElementById('stat-tasks').textContent = totalTasks + '+';
        document.getElementById('stat-credits').textContent = totalCredits + '+';
    },

    updateStatsAnon(data) {
        document.getElementById('stat-users').textContent = data.total;
        let tasksOnPage = 0;
        (data.sessions || []).forEach(s => { tasksOnPage += (s.total_tasks || 0); });
        document.getElementById('stat-tasks').textContent = String(tasksOnPage);
        document.getElementById('stat-credits').textContent = '-';
    },

    updateStatsTasks(data) {
        const sc = data.status_counts || {};
        document.getElementById('stat-users').textContent = sc.processing || 0;
        document.getElementById('stat-tasks').textContent = sc.created || 0;
        document.getElementById('stat-credits').textContent = sc.done || 0;
    },

    async loadAllTasks() {
        const tbody = document.getElementById('admin-table-body');
        tbody.innerHTML = '<tr><td colspan="8" style="text-align: center; color: var(--text-muted);">Loading...</td></tr>';
        try {
            const params = new URLSearchParams({
                page: this.currentPage,
                per_page: this.perPage,
                sort_by: this.sortBy,
                sort_desc: this.sortDesc
            });
            if (this.tasksStatusFilter) params.append('status', this.tasksStatusFilter);

            const response = await fetch(`/api/admin/tasks?${params}`);
            if (response.status === 403) {
                window.location.href = '/';
                return;
            }
            if (!response.ok) throw new Error('Failed to load tasks');

            const data = await response.json();
            this.totalItems = data.total;
            this.statusCounts = data.status_counts || {};
            this.renderAllTasks(data.tasks || []);
            this.updatePagination();
            this.updateStatsTasks(data);
        } catch (e) {
            console.error('Error loading tasks:', e);
            tbody.innerHTML = '<tr><td colspan="8" style="text-align: center; color: var(--error);">Failed to load tasks</td></tr>';
        }
    },

    renderAllTasks(tasks) {
        const tbody = document.getElementById('admin-table-body');
        if (!tasks || tasks.length === 0) {
            tbody.innerHTML = '<tr><td colspan="8" style="text-align:center; color: var(--text-muted);">No tasks found</td></tr>';
            return;
        }

        tbody.innerHTML = tasks.map(t => {
            const statusColor = {
                processing: 'var(--info)',
                created: 'var(--warning)',
                done: 'var(--success)',
                error: 'var(--error)'
            }[t.status] || 'var(--text-muted)';

            const workerPort = t.worker_api ? t.worker_api.split(':').pop().split('/')[0] : '-';
            const ownerDisplay = t.owner_name || t.owner_id;
            const thumbUrl = `/api/thumb/${t.task_id}`;

            return `
                <tr>
                    <td>
                        <img src="${thumbUrl}" alt="" style="width:50px; height:50px; object-fit:cover; border-radius:6px; background:#222;"
                             onerror="this.src='/static/images/placeholder-thumb.svg'">
                    </td>
                    <td style="font-family: monospace; font-size: 0.8rem;" title="${t.task_id}">${t.task_id.substring(0, 8)}...</td>
                    <td>
                        <span style="font-size: 0.85rem;">${ownerDisplay}</span>
                        <div style="font-size: 0.7rem; color: var(--text-muted);">${t.owner_type}</div>
                    </td>
                    <td>
                        <span style="color: ${statusColor}; font-weight: 600;">${t.status}</span>
                        ${t.retry_count > 0 ? `<div style="font-size: 0.7rem; color: var(--warning);">retry: ${t.retry_count}</div>` : ''}
                    </td>
                    <td>${t.ready_count}/${t.total_count}</td>
                    <td style="font-size: 0.8rem;">${workerPort}</td>
                    <td style="font-size: 0.85rem;">${this.formatDate(t.created_at)}</td>
                    <td>
                        <div style="display: flex; gap: 0.25rem;">
                            <a href="/task?id=${t.task_id}" target="_blank" class="btn btn-secondary" style="padding: 0.2rem 0.5rem; font-size: 0.75rem;">Open</a>
                            <button class="btn btn-secondary" style="padding: 0.2rem 0.5rem; font-size: 0.75rem; color: var(--error); border-color: var(--error);"
                                    onclick="Admin.deleteTask('${t.task_id}')">Delete</button>
                        </div>
                    </td>
                </tr>
            `;
        }).join('');
    },

    async deleteTask(taskId) {
        if (!confirm(`Delete task ${taskId.substring(0, 8)}...? This cannot be undone.`)) return;

        try {
            const response = await fetch(`/api/admin/task/${taskId}`, { method: 'DELETE' });
            if (!response.ok) {
                const data = await response.json();
                throw new Error(data.detail || 'Failed to delete task');
            }
            alert('Task deleted');
            this.loadList();
        } catch (e) {
            console.error('Delete error:', e);
            alert('Failed to delete task: ' + e.message);
        }
    },

    async loadAnonSessions() {
        const tbody = document.getElementById('admin-table-body');
        tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: var(--text-muted);">Loading...</td></tr>';
        try {
            const params = new URLSearchParams({
                page: this.currentPage,
                per_page: this.perPage,
                sort_by: this.sortBy,
                sort_desc: this.sortDesc
            });
            if (this.searchQuery) params.append('query', this.searchQuery);

            const response = await fetch(`/api/admin/anon-sessions?${params}`);
            if (response.status === 403) {
                window.location.href = '/';
                return;
            }
            if (!response.ok) throw new Error('Failed to load anon sessions');
            const data = await response.json();
            this.totalItems = data.total;
            this.renderAnonSessions(data.sessions || []);
            this.updatePagination();
            this.updateStatsAnon(data);
        } catch (e) {
            console.error('Error loading anon sessions:', e);
            tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: var(--error);">Failed to load anon sessions</td></tr>';
        }
    },

    renderAnonSessions(sessions) {
        const tbody = document.getElementById('admin-table-body');
        if (!sessions || sessions.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" style="text-align:center; color: var(--text-muted);">No anon sessions found</td></tr>';
            return;
        }
        tbody.innerHTML = sessions.map(s => `
            <tr style="cursor: pointer;" data-anon-id="${s.anon_id}">
                <td style="font-family: monospace; font-size: 0.875rem;" title="${s.anon_id}">${s.anon_id.substring(0, 8)}...</td>
                <td>${s.free_used}</td>
                <td>${s.total_tasks}</td>
                <td>${this.formatDate(s.created_at)}</td>
                <td>${this.formatDate(s.last_seen_at)}</td>
                <td>
                    <button class="btn btn-secondary" style="padding: 0.25rem 0.5rem; font-size: 0.75rem;"
                            onclick="event.stopPropagation(); Admin.openAnonTasksModal('${s.anon_id}')">
                        View Tasks
                    </button>
                </td>
            </tr>
        `).join('');

        tbody.querySelectorAll('tr[data-anon-id]').forEach(row => {
            row.addEventListener('click', (e) => {
                if (e.target.closest('button')) return;
                const anonId = row.getAttribute('data-anon-id');
                this.openAnonTasksModal(anonId);
            });
        });
    },
    
    openBalanceModal(userId, email, currentBalance) {
        this.selectedUser = { id: userId, email, balance: currentBalance };
        
        document.getElementById('modal-email').textContent = email;
        document.getElementById('modal-current').textContent = currentBalance;
        document.getElementById('modal-set-balance').value = '';
        document.getElementById('modal-delta').value = '';
        
        const modal = document.getElementById('balance-modal');
        modal.classList.remove('hidden');
        modal.style.display = 'flex';
    },
    
    closeModal() {
        const modal = document.getElementById('balance-modal');
        modal.classList.add('hidden');
        modal.style.display = 'none';
        this.selectedUser = null;
    },
    
    async saveBalance() {
        if (!this.selectedUser) return;
        
        const setBalance = document.getElementById('modal-set-balance').value;
        const delta = document.getElementById('modal-delta').value;
        
        const body = {};
        if (setBalance !== '') {
            body.set_to = parseInt(setBalance);
        } else if (delta !== '') {
            body.delta = parseInt(delta);
        } else {
            alert('Please enter a value');
            return;
        }
        
        try {
            const response = await fetch(`/api/admin/user/${this.selectedUser.id}/balance`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(body)
            });
            
            if (!response.ok) {
                throw new Error('Failed to update balance');
            }
            
            const result = await response.json();
            alert(`Balance updated: ${result.old_balance} → ${result.new_balance}`);
            
            this.closeModal();
            this.loadList();
        } catch (error) {
            console.error('Error saving balance:', error);
            alert('Failed to update balance');
        }
    },
    
    formatDate(dateStr) {
        if (!dateStr) return '-';
        const date = new Date(dateStr);
        return date.toLocaleDateString() + ' ' + date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    },
    
    openUserTasksModal(userId, email) {
        this.selectedTasksOwner = { type: 'user', id: userId, label: email };
        this.tasksCurrentPage = 1;
        
        const titleEl = document.getElementById('tasks-modal-title');
        if (titleEl) titleEl.textContent = 'User Tasks';
        document.getElementById('tasks-modal-email').textContent = email;
        
        const modal = document.getElementById('tasks-modal');
        modal.classList.remove('hidden');
        modal.style.display = 'flex';
        
        // Load tasks
        this.loadOwnerTasks();
    },

    openAnonTasksModal(anonId) {
        this.selectedTasksOwner = { type: 'anon', id: anonId, label: anonId };
        this.tasksCurrentPage = 1;
        const titleEl = document.getElementById('tasks-modal-title');
        if (titleEl) titleEl.textContent = 'Anon Session Tasks';
        document.getElementById('tasks-modal-email').textContent = anonId;

        const modal = document.getElementById('tasks-modal');
        modal.classList.remove('hidden');
        modal.style.display = 'flex';

        this.loadOwnerTasks();
    },
    
    closeTasksModal() {
        const modal = document.getElementById('tasks-modal');
        modal.classList.add('hidden');
        modal.style.display = 'none';
        this.selectedTasksOwner = null;
        this.tasksCurrentPage = 1;
        this.tasksTotal = 0;
    },

    async loadOwnerTasks() {
        const owner = this.selectedTasksOwner;
        if (!owner) return;
        if (owner.type === 'user') return await this.loadUserTasks(owner.id);
        return await this.loadAnonTasks(owner.id);
    },
    
    async loadUserTasks(userId) {
        const tbody = document.getElementById('tasks-table');
        tbody.innerHTML = '<tr><td colspan="8" style="text-align: center; color: var(--text-muted);">Loading...</td></tr>';
        
        try {
            const params = new URLSearchParams({
                page: this.tasksCurrentPage,
                per_page: this.tasksPerPage
            });
            
            const response = await fetch(`/api/admin/user/${userId}/tasks?${params}`);
            
            if (response.status === 403) {
                window.location.href = '/';
                return;
            }
            
            if (!response.ok) {
                throw new Error('Failed to load tasks');
            }
            
            const data = await response.json();
            this.tasksTotal = data.total;
            
            this.renderUserTasks(data.tasks);
            this.updateTasksPagination();
        } catch (error) {
            console.error('Error loading tasks:', error);
            tbody.innerHTML = '<tr><td colspan="8" style="text-align: center; color: var(--error);">Failed to load tasks</td></tr>';
        }
    },

    async loadAnonTasks(anonId) {
        const tbody = document.getElementById('tasks-table');
        tbody.innerHTML = '<tr><td colspan="8" style="text-align: center; color: var(--text-muted);">Loading...</td></tr>';

        try {
            const params = new URLSearchParams({
                page: this.tasksCurrentPage,
                per_page: this.tasksPerPage
            });

            const response = await fetch(`/api/admin/anon-session/${encodeURIComponent(anonId)}/tasks?${params}`);
            if (response.status === 403) {
                window.location.href = '/';
                return;
            }
            if (!response.ok) throw new Error('Failed to load tasks');
            const data = await response.json();
            this.tasksTotal = data.total;
            this.renderUserTasks(data.tasks); // same shape
            this.updateTasksPagination();
        } catch (error) {
            console.error('Error loading anon tasks:', error);
            tbody.innerHTML = '<tr><td colspan="8" style="text-align: center; color: var(--error);">Failed to load tasks</td></tr>';
        }
    },
    
    renderUserTasks(tasks) {
        const tbody = document.getElementById('tasks-table');
        
        if (tasks.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="8" style="text-align: center; color: var(--text-muted);">No tasks found</td>
                </tr>
            `;
            return;
        }
        
        tbody.innerHTML = tasks.map(task => {
            const statusColor = {
                'created': 'var(--text-muted)',
                'processing': 'var(--accent)',
                'done': 'var(--success)',
                'error': 'var(--error)'
            }[task.status] || 'var(--text-muted)';
            
            const progressPercent = task.progress || 0;
            const progressBar = `
                <div style="display: flex; align-items: center; gap: 0.5rem;">
                    <div style="flex: 1; height: 8px; background: var(--bg-input); border-radius: 4px; overflow: hidden;">
                        <div style="height: 100%; width: ${progressPercent}%; background: ${statusColor}; transition: width 0.3s;"></div>
                    </div>
                    <span style="font-size: 0.875rem; color: var(--text-secondary); min-width: 45px;">${progressPercent}%</span>
                </div>
            `;
            
            const inputUrlDisplay = task.input_url 
                ? `<a href="${task.input_url}" target="_blank" style="color: var(--accent); text-decoration: none; max-width: 200px; display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${task.input_url}">${task.input_url.substring(0, 30)}...</a>`
                : '-';
            
            return `
                <tr>
                    <td style="font-family: monospace; font-size: 0.875rem;">${task.task_id.substring(0, 8)}...</td>
                    <td>
                        <span style="color: ${statusColor}; font-weight: 500; text-transform: capitalize;">${task.status}</span>
                    </td>
                    <td style="min-width: 150px;">${progressBar}</td>
                    <td style="font-size: 0.875rem;">${this.formatDate(task.created_at)}</td>
                    <td style="font-size: 0.875rem;">${this.formatDate(task.updated_at)}</td>
                    <td style="max-width: 200px;">${inputUrlDisplay}</td>
                    <td style="text-align: center;">
                        <span style="color: var(--text-secondary);">${task.ready_count}/${task.total_count}</span>
                    </td>
                    <td>
                        <div class="flex gap-1" style="flex-wrap: wrap;">
                            <a href="/task?id=${task.task_id}" target="_blank" class="btn btn-secondary" style="padding: 0.25rem 0.5rem; font-size: 0.75rem;">
                                View Task
                            </a>
                            <button class="btn btn-secondary" style="padding: 0.25rem 0.5rem; font-size: 0.75rem;"
                                    onclick="Admin.showTaskOwner('${task.task_id}', this)">
                                Owner
                            </button>
                            <button class="btn btn-ghost" style="padding: 0.25rem 0.5rem; font-size: 0.75rem;"
                                    onclick="Admin.restartTask('${task.task_id}', this)">
                                Restart
                            </button>
                            <button class="btn btn-ghost" style="padding: 0.25rem 0.5rem; font-size: 0.75rem; color: var(--error); border-color: rgba(239,71,111,0.35);"
                                    onclick="Admin.deleteTask('${task.task_id}', this)">
                                Delete
                            </button>
                        </div>
                    </td>
                </tr>
            `;
        }).join('');
    },

    async showTaskOwner(taskId, btnEl) {
        if (!taskId) return;
        if (btnEl) btnEl.disabled = true;
        try {
            const resp = await fetch(`/api/admin/task/${taskId}/owner`);
            const data = await resp.json();
            if (!resp.ok) {
                alert(data.detail || 'Failed to load owner');
                return;
            }
            const who = data.owner_type === 'user'
                ? `user: ${data.owner_id}${data.user_id ? ` (id=${data.user_id})` : ''}`
                : `anon: ${data.owner_id}`;
            alert(`${taskId}\n${who}`);
        } catch (e) {
            console.error('Owner lookup error:', e);
            alert('Failed to load owner');
        } finally {
            if (btnEl) btnEl.disabled = false;
        }
    },

    async restartTask(taskId, btnEl) {
        if (!taskId) return;
        const ok = confirm(`Restart task ${taskId}?`);
        if (!ok) return;

        if (btnEl) btnEl.disabled = true;
        try {
            const resp = await fetch(`/api/task/${taskId}/restart`, { method: 'POST' });
            const data = await resp.json();

            if (!resp.ok) {
                alert(data.detail || 'Failed to restart task');
                return;
            }

            alert(`Task restarted: ${taskId} (${data.status})`);

            // Refresh tasks list
            await this.loadOwnerTasks();
        } catch (e) {
            console.error('Restart task error:', e);
            alert('Failed to restart task');
        } finally {
            if (btnEl) btnEl.disabled = false;
        }
    },

    async deleteTask(taskId, btnEl) {
        if (!taskId) return;
        const ok = confirm(`Delete task ${taskId}? This cannot be undone.`);
        if (!ok) return;

        if (btnEl) btnEl.disabled = true;
        try {
            const resp = await fetch(`/api/admin/task/${taskId}`, { method: 'DELETE' });
            const raw = await resp.text();
            let data = {};
            try { data = raw ? JSON.parse(raw) : {}; } catch (_) { data = {}; }

            if (!resp.ok) {
                alert(data.detail || raw || 'Failed to delete task');
                return;
            }

            alert(`Task deleted: ${taskId}`);
            await this.loadOwnerTasks();
        } catch (e) {
            console.error('Delete task error:', e);
            alert('Failed to delete task');
        } finally {
            if (btnEl) btnEl.disabled = false;
        }
    },
    
    updateTasksPagination() {
        const totalPages = Math.ceil(this.tasksTotal / this.tasksPerPage);
        const start = (this.tasksCurrentPage - 1) * this.tasksPerPage + 1;
        const end = Math.min(this.tasksCurrentPage * this.tasksPerPage, this.tasksTotal);
        
        document.getElementById('tasks-pagination-info').textContent = 
            `Showing ${start}-${end} of ${this.tasksTotal} tasks`;
        
        document.getElementById('tasks-prev-page').disabled = this.tasksCurrentPage <= 1;
        document.getElementById('tasks-next-page').disabled = this.tasksCurrentPage >= totalPages;
    }
};

document.addEventListener('DOMContentLoaded', () => Admin.init());

