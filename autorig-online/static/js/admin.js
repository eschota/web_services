/**
 * AutoRig Online - Admin Panel
 */

const Admin = {
    // Users tab state
    currentPage: 1,
    perPage: 20,
    totalUsers: 0,
    sortBy: 'created_at',
    sortDesc: true,
    searchQuery: '',
    selectedUser: null,
    
    // User tasks modal state
    tasksCurrentPage: 1,
    tasksPerPage: 20,
    tasksTotal: 0,
    selectedTasksUser: null,
    
    // All tasks tab state
    allTasksCurrentPage: 1,
    allTasksPerPage: 20,
    allTasksTotal: 0,
    allTasksStatusFilter: '',
    allTasksSearchQuery: '',
    allTasksSortBy: 'created_at',
    allTasksSortDesc: true,

    // Workers tab state
    workers: [],
    
    // Current tab
    currentTab: 'users',
    
    async init() {
        // Init i18n
        await I18n.init();
        
        // Setup theme
        this.setupTheme();
        
        // Load stats
        await this.loadStats();
        
        // Load users (default tab)
        await this.loadUsers();
        
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

        // Delete ALL tasks (dangerous!)
        const deleteAllBtn = document.getElementById('delete-all-tasks-btn');
        deleteAllBtn?.addEventListener('click', async () => {
            // First confirmation
            const ok1 = confirm('⚠️ WARNING!\n\nYou are about to DELETE ALL TASKS from the database.\n\nThis action CANNOT be undone!\n\nAre you sure?');
            if (!ok1) return;
            
            // Second confirmation with typing
            const confirmText = prompt('⚠️ FINAL WARNING!\n\nTo confirm deletion of ALL tasks, type "DELETE ALL" below:');
            if (confirmText !== 'DELETE ALL') {
                alert('Cancelled. Text did not match.');
                return;
            }

            deleteAllBtn.disabled = true;
            deleteAllBtn.textContent = 'Deleting...';
            try {
                const resp = await fetch('/api/admin/tasks/all', { method: 'DELETE' });
                const data = await resp.json();
                if (!resp.ok) {
                    alert(data.detail || 'Failed to delete tasks');
                    deleteAllBtn.disabled = false;
                    deleteAllBtn.textContent = '🗑️ Delete ALL Tasks';
                    return;
                }

                alert(`✅ Deleted ${data.deleted_count} tasks.\n\nService restarting... Page will reload in ~5 seconds.`);
                setTimeout(() => window.location.reload(), 5000);
            } catch (e) {
                console.error('Delete all tasks error:', e);
                alert('Failed to delete tasks');
                deleteAllBtn.disabled = false;
                deleteAllBtn.textContent = '🗑️ Delete ALL Tasks';
            }
        });

        // Restart all incomplete tasks
        const restartIncompleteBtn = document.getElementById('restart-incomplete-btn');
        restartIncompleteBtn?.addEventListener('click', async () => {
            const ok = confirm('🔄 Restart all incomplete tasks?\n\nThis will restart all tasks with status: created, processing, error.\n\nContinue?');
            if (!ok) return;

            restartIncompleteBtn.disabled = true;
            restartIncompleteBtn.textContent = '⏳ Restarting...';
            try {
                const resp = await fetch('/api/admin/tasks/restart-incomplete', { method: 'POST' });
                const text = await resp.text();
                let data;
                try {
                    data = JSON.parse(text);
                } catch {
                    alert(`Server error: ${text}`);
                    return;
                }
                
                if (!resp.ok) {
                    alert(data.detail || 'Failed to restart tasks');
                    return;
                }

                alert(`✅ ${data.message}\n\nRefresh the page in a few seconds to see updated statuses.`);
                
                // Refresh stats and tasks
                await Admin.loadStats();
                if (Admin.currentTab === 'tasks') {
                    await Admin.loadAllTasks();
                }
            } catch (e) {
                console.error('Restart incomplete tasks error:', e);
                alert('Failed to restart tasks: ' + e.message);
            } finally {
                restartIncompleteBtn.disabled = false;
                restartIncompleteBtn.textContent = '🔄 Restart Incomplete';
            }
        });

        // Tab switching
        document.querySelectorAll('.admin-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                const tabName = tab.getAttribute('data-tab');
                this.switchTab(tabName);
            });
        });

        // Users search
        const searchInput = document.getElementById('search-input');
        let searchTimeout;
        searchInput.addEventListener('input', () => {
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(() => {
                this.searchQuery = searchInput.value;
                this.currentPage = 1;
                this.loadUsers();
            }, 300);
        });
        
        // Users sort headers
        document.querySelectorAll('[data-sort]').forEach(th => {
            th.addEventListener('click', () => {
                const field = th.getAttribute('data-sort');
                if (this.sortBy === field) {
                    this.sortDesc = !this.sortDesc;
                } else {
                    this.sortBy = field;
                    this.sortDesc = true;
                }
                this.loadUsers();
            });
        });
        
        // Users pagination
        document.getElementById('prev-page').addEventListener('click', () => {
            if (this.currentPage > 1) {
                this.currentPage--;
                this.loadUsers();
            }
        });
        
        document.getElementById('next-page').addEventListener('click', () => {
            const totalPages = Math.ceil(this.totalUsers / this.perPage);
            if (this.currentPage < totalPages) {
                this.currentPage++;
                this.loadUsers();
            }
        });
        
        // Balance modal
        document.getElementById('modal-cancel').addEventListener('click', () => this.closeModal());
        document.getElementById('modal-save').addEventListener('click', () => this.saveBalance());
        
        // Close modal on backdrop click
        document.getElementById('balance-modal').addEventListener('click', (e) => {
            if (e.target.id === 'balance-modal') {
                this.closeModal();
            }
        });
        
        // User tasks modal
        document.getElementById('tasks-modal-close').addEventListener('click', () => this.closeTasksModal());
        document.getElementById('tasks-modal').addEventListener('click', (e) => {
            if (e.target.id === 'tasks-modal') {
                this.closeTasksModal();
            }
        });
        
        // User tasks pagination
        document.getElementById('tasks-prev-page').addEventListener('click', () => {
            if (this.tasksCurrentPage > 1) {
                this.tasksCurrentPage--;
                this.loadUserTasks(this.selectedTasksUser.id);
            }
        });
        
        document.getElementById('tasks-next-page').addEventListener('click', () => {
            const totalPages = Math.ceil(this.tasksTotal / this.tasksPerPage);
            if (this.tasksCurrentPage < totalPages) {
                this.tasksCurrentPage++;
                this.loadUserTasks(this.selectedTasksUser.id);
            }
        });
        
        // All tasks tab: status filter
        const statusFilter = document.getElementById('tasks-status-filter');
        statusFilter?.addEventListener('change', () => {
            this.allTasksStatusFilter = statusFilter.value;
            this.allTasksCurrentPage = 1;
            this.loadAllTasks();
        });
        
        // All tasks tab: search
        const tasksSearchInput = document.getElementById('tasks-search-input');
        let tasksSearchTimeout;
        tasksSearchInput?.addEventListener('input', () => {
            clearTimeout(tasksSearchTimeout);
            tasksSearchTimeout = setTimeout(() => {
                this.allTasksSearchQuery = tasksSearchInput.value;
                this.allTasksCurrentPage = 1;
                this.loadAllTasks();
            }, 300);
        });
        
        // All tasks sort headers
        document.querySelectorAll('[data-sort-tasks]').forEach(th => {
            th.addEventListener('click', () => {
                const field = th.getAttribute('data-sort-tasks');
                if (this.allTasksSortBy === field) {
                    this.allTasksSortDesc = !this.allTasksSortDesc;
                } else {
                    this.allTasksSortBy = field;
                    this.allTasksSortDesc = true;
                }
                this.loadAllTasks();
            });
        });
        
        // All tasks pagination
        document.getElementById('all-tasks-prev-page')?.addEventListener('click', () => {
            if (this.allTasksCurrentPage > 1) {
                this.allTasksCurrentPage--;
                this.loadAllTasks();
            }
        });
        
        document.getElementById('all-tasks-next-page')?.addEventListener('click', () => {
            const totalPages = Math.ceil(this.allTasksTotal / this.allTasksPerPage);
            if (this.allTasksCurrentPage < totalPages) {
                this.allTasksCurrentPage++;
                this.loadAllTasks();
            }
        });

        // Workers tab: add worker form
        const addWorkerForm = document.getElementById('add-worker-form');
        addWorkerForm?.addEventListener('submit', async (e) => {
            e.preventDefault();
            const urlEl = document.getElementById('new-worker-url');
            const weightEl = document.getElementById('new-worker-weight');
            const enabledEl = document.getElementById('new-worker-enabled');

            const url = (urlEl?.value || '').trim();
            const weight = parseInt(weightEl?.value || '0', 10) || 0;
            const enabled = !!enabledEl?.checked;

            if (!url) {
                alert('Worker URL is required');
                return;
            }

            await this.createWorker({ url, weight, enabled });

            // Clear URL for convenience; keep weight/enabled as-is
            if (urlEl) urlEl.value = '';
        });
    },
    
    switchTab(tabName) {
        this.currentTab = tabName;
        
        // Update tab buttons
        document.querySelectorAll('.admin-tab').forEach(tab => {
            const isActive = tab.getAttribute('data-tab') === tabName;
            tab.classList.toggle('active', isActive);
            tab.style.borderBottomColor = isActive ? 'var(--accent)' : 'transparent';
            tab.style.color = isActive ? 'var(--accent)' : 'var(--text-muted)';
        });
        
        // Show/hide tab content
        document.querySelectorAll('.tab-content').forEach(content => {
            content.style.display = 'none';
        });
        document.getElementById(`tab-${tabName}`).style.display = 'block';
        
        // Load data for tab
        if (tabName === 'tasks') {
            this.loadAllTasks();
        } else if (tabName === 'workers') {
            this.loadWorkers();
        }
    },
    
    async loadStats() {
        try {
            const response = await fetch('/api/admin/stats');
            
            if (response.status === 403) {
                window.location.href = '/';
                return;
            }
            
            if (!response.ok) {
                throw new Error('Failed to load stats');
            }
            
            const data = await response.json();
            
            document.getElementById('stat-users').textContent = data.total_users;
            document.getElementById('stat-tasks').textContent = data.total_tasks;
            document.getElementById('stat-credits').textContent = data.total_credits;
            
            // Status breakdown
            document.getElementById('stat-processing').textContent = data.tasks_by_status.processing || 0;
            document.getElementById('stat-done').textContent = data.tasks_by_status.done || 0;
            document.getElementById('stat-error').textContent = data.tasks_by_status.error || 0;
        } catch (error) {
            console.error('Error loading stats:', error);
        }
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
            this.totalUsers = data.total;
            
            this.renderUsers(data.users);
            this.updatePagination();
        } catch (error) {
            console.error('Error loading users:', error);
        }
    },
    
    renderUsers(users) {
        const tbody = document.getElementById('users-table');
        
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
                this.openTasksModal(userId, userEmail);
            });
        });
    },
    
    updatePagination() {
        const totalPages = Math.ceil(this.totalUsers / this.perPage);
        const start = (this.currentPage - 1) * this.perPage + 1;
        const end = Math.min(this.currentPage * this.perPage, this.totalUsers);
        
        document.getElementById('pagination-info').textContent = 
            `Showing ${start}-${end} of ${this.totalUsers} users`;
        
        document.getElementById('prev-page').disabled = this.currentPage <= 1;
        document.getElementById('next-page').disabled = this.currentPage >= totalPages;
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
            this.loadUsers();
            this.loadStats(); // Refresh stats
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
    
    openTasksModal(userId, email) {
        this.selectedTasksUser = { id: userId, email };
        this.tasksCurrentPage = 1;
        
        document.getElementById('tasks-modal-email').textContent = email;
        
        const modal = document.getElementById('tasks-modal');
        modal.classList.remove('hidden');
        modal.style.display = 'flex';
        
        // Load tasks
        this.loadUserTasks(userId);
    },
    
    closeTasksModal() {
        const modal = document.getElementById('tasks-modal');
        modal.classList.add('hidden');
        modal.style.display = 'none';
        this.selectedTasksUser = null;
        this.tasksCurrentPage = 1;
        this.tasksTotal = 0;
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
            if (this.selectedTasksUser?.id) {
                await this.loadUserTasks(this.selectedTasksUser.id);
            }
            
            // Also refresh all tasks if on that tab
            if (this.currentTab === 'tasks') {
                await this.loadAllTasks();
            }
            
            // Refresh stats
            await this.loadStats();
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
            
            if (this.selectedTasksUser?.id) {
                await this.loadUserTasks(this.selectedTasksUser.id);
            }
            
            // Also refresh all tasks if on that tab
            if (this.currentTab === 'tasks') {
                await this.loadAllTasks();
            }
            
            // Refresh stats
            await this.loadStats();
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
    },
    
    // =========================================================================
    // All Tasks Tab
    // =========================================================================
    async loadAllTasks() {
        const tbody = document.getElementById('all-tasks-table');
        tbody.innerHTML = '<tr><td colspan="9" style="text-align: center; color: var(--text-muted);">Loading...</td></tr>';
        
        try {
            const params = new URLSearchParams({
                page: this.allTasksCurrentPage,
                per_page: this.allTasksPerPage,
                sort_by: this.allTasksSortBy,
                sort_desc: this.allTasksSortDesc
            });
            
            if (this.allTasksStatusFilter) {
                params.append('status', this.allTasksStatusFilter);
            }
            
            if (this.allTasksSearchQuery) {
                params.append('query', this.allTasksSearchQuery);
            }
            
            const response = await fetch(`/api/admin/tasks?${params}`);
            
            if (response.status === 403) {
                window.location.href = '/';
                return;
            }
            
            if (!response.ok) {
                throw new Error('Failed to load tasks');
            }
            
            const data = await response.json();
            this.allTasksTotal = data.total;
            
            this.renderAllTasks(data.tasks);
            this.updateAllTasksPagination();
        } catch (error) {
            console.error('Error loading all tasks:', error);
            tbody.innerHTML = '<tr><td colspan="9" style="text-align: center; color: var(--error);">Failed to load tasks</td></tr>';
        }
    },

    // =========================================================================
    // Workers Tab
    // =========================================================================
    async loadWorkers() {
        const tbody = document.getElementById('workers-table');
        if (tbody) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="9" style="text-align: center; color: var(--text-muted);">Loading...</td>
                </tr>
            `;
        }

        try {
            const resp = await fetch('/api/admin/workers');
            if (resp.status === 403) {
                window.location.href = '/';
                return;
            }
            const data = await resp.json();
            if (!resp.ok) {
                alert(data.detail || 'Failed to load workers');
                return;
            }
            this.workers = data.workers || [];
            this.renderWorkers(this.workers);
        } catch (e) {
            console.error('Error loading workers:', e);
            alert('Failed to load workers');
        }
    },

    renderWorkers(workers) {
        const tbody = document.getElementById('workers-table');
        if (!tbody) return;

        const total = workers.length;
        const enabledCount = workers.filter(w => w.enabled).length;
        const totalEl = document.getElementById('workers-total');
        const enabledEl = document.getElementById('workers-enabled');
        if (totalEl) totalEl.textContent = String(total);
        if (enabledEl) enabledEl.textContent = String(enabledCount);

        if (workers.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="9" style="text-align: center; color: var(--text-muted);">No workers configured</td>
                </tr>
            `;
            return;
        }

        tbody.innerHTML = workers.map(w => `
            <tr data-worker-id="${w.id}">
                <td>${w.id}</td>
                <td>
                    <input class="form-input" id="worker-url-${w.id}" value="${this.escapeHtml(w.url)}" style="width: 100%;">
                </td>
                <td>
                    <input class="form-input" type="number" id="worker-weight-${w.id}" value="${w.weight}" style="width: 100px;">
                </td>
                <td style="text-align: center;">
                    <input type="checkbox" id="worker-enabled-${w.id}" ${w.enabled ? 'checked' : ''} style="width: 18px; height: 18px;">
                </td>
                <td style="text-align: right; font-variant-numeric: tabular-nums;">${w.done_tasks ?? 0}</td>
                <td style="text-align: right; font-variant-numeric: tabular-nums;">${w.total_tasks ?? 0}</td>
                <td style="text-align: right; font-variant-numeric: tabular-nums;">${this.formatPct(w.done_share_pct)}</td>
                <td>${this.formatDate(w.updated_at)}</td>
                <td style="white-space: nowrap;">
                    <button class="btn btn-secondary" data-action="save-worker" data-worker-id="${w.id}" style="padding: 0.25rem 0.5rem; font-size: 0.75rem;">
                        Save
                    </button>
                    <button class="btn btn-ghost" data-action="delete-worker" data-worker-id="${w.id}" style="padding: 0.25rem 0.5rem; font-size: 0.75rem; color: var(--error);">
                        Delete
                    </button>
                </td>
            </tr>
        `).join('');

        tbody.querySelectorAll('button[data-action="save-worker"]').forEach(btn => {
            btn.addEventListener('click', async () => {
                const workerId = parseInt(btn.getAttribute('data-worker-id'), 10);
                const urlEl = document.getElementById(`worker-url-${workerId}`);
                const weightEl = document.getElementById(`worker-weight-${workerId}`);
                const enabledEl = document.getElementById(`worker-enabled-${workerId}`);

                const url = (urlEl?.value || '').trim();
                const weight = parseInt(weightEl?.value || '0', 10) || 0;
                const enabled = !!enabledEl?.checked;

                btn.disabled = true;
                try {
                    await this.updateWorker(workerId, { url, weight, enabled });
                } finally {
                    btn.disabled = false;
                }
            });
        });

        tbody.querySelectorAll('button[data-action="delete-worker"]').forEach(btn => {
            btn.addEventListener('click', async () => {
                const workerId = parseInt(btn.getAttribute('data-worker-id'), 10);
                const ok = confirm(`Delete worker #${workerId}?`);
                if (!ok) return;
                btn.disabled = true;
                try {
                    await this.deleteWorker(workerId);
                } finally {
                    btn.disabled = false;
                }
            });
        });
    },

    async createWorker(payload) {
        const btn = document.getElementById('add-worker-btn');
        if (btn) btn.disabled = true;
        try {
            const resp = await fetch('/api/admin/workers', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok) {
                alert(data.detail || 'Failed to create worker');
                return;
            }
            await this.loadWorkers();
        } catch (e) {
            console.error('Create worker error:', e);
            alert('Failed to create worker');
        } finally {
            if (btn) btn.disabled = false;
        }
    },

    async updateWorker(workerId, payload) {
        try {
            const resp = await fetch(`/api/admin/workers/${workerId}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok) {
                alert(data.detail || 'Failed to update worker');
                return;
            }
            await this.loadWorkers();
        } catch (e) {
            console.error('Update worker error:', e);
            alert('Failed to update worker');
        }
    },

    async deleteWorker(workerId) {
        try {
            const resp = await fetch(`/api/admin/workers/${workerId}`, { method: 'DELETE' });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok) {
                alert(data.detail || 'Failed to delete worker');
                return;
            }
            await this.loadWorkers();
        } catch (e) {
            console.error('Delete worker error:', e);
            alert('Failed to delete worker');
        }
    },
    
    renderAllTasks(tasks) {
        const tbody = document.getElementById('all-tasks-table');
        
        if (tasks.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="7" style="text-align: center; color: var(--text-muted);">No tasks found</td>
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
                <div style="display: flex; align-items: center; gap: 0.25rem;">
                    <div style="flex: 1; height: 6px; background: var(--bg-input); border-radius: 3px; overflow: hidden; min-width: 50px;">
                        <div style="height: 100%; width: ${progressPercent}%; background: ${statusColor}; transition: width 0.3s;"></div>
                    </div>
                    <span style="font-size: 0.7rem; color: var(--text-secondary); min-width: 30px;">${progressPercent}%</span>
                </div>
            `;
            
            // Preview thumbnail (video poster if done, placeholder otherwise)
            const preview = task.video_ready 
                ? `<img src="/api/thumb/${task.task_id}" style="width: 50px; height: 50px; object-fit: cover; border-radius: 4px;" onerror="this.style.display='none'">`
                : `<div style="width: 50px; height: 50px; background: var(--bg-input); border-radius: 4px; display: flex; align-items: center; justify-content: center; color: var(--text-muted); font-size: 0.7rem;">${task.status === 'done' ? '✓' : '...'}</div>`;
            
            // Owner display (truncate if too long)
            const ownerDisplay = task.owner_id.length > 20 
                ? task.owner_id.substring(0, 17) + '...' 
                : task.owner_id;
            const ownerIcon = task.owner_type === 'user' ? '👤' : '👻';
            
            return `
                <tr>
                    <td style="padding: 0.5rem;">${preview}</td>
                    <td style="font-family: monospace; font-size: 0.75rem;">
                        <a href="/task?id=${task.task_id}" target="_blank" style="color: var(--accent); text-decoration: none;" title="${task.task_id}">
                            ${task.task_id.substring(0, 8)}...
                        </a>
                    </td>
                    <td style="font-size: 0.75rem;" title="${task.owner_id}">
                        ${ownerIcon} ${ownerDisplay}
                    </td>
                    <td>
                        <span style="color: ${statusColor}; font-weight: 500; text-transform: capitalize; font-size: 0.8rem;">${task.status}</span>
                    </td>
                    <td>${progressBar}</td>
                    <td style="font-size: 0.75rem;">${this.formatDate(task.created_at)}</td>
                    <td>
                        <div style="display: flex; gap: 0.25rem;">
                            <button class="btn btn-ghost" style="padding: 0.2rem 0.4rem; font-size: 0.75rem; min-width: unset;"
                                    onclick="Admin.restartTaskFromList('${task.task_id}', this)" title="Restart">
                                ↻
                            </button>
                            <button class="btn btn-ghost" style="padding: 0.2rem 0.4rem; font-size: 0.75rem; min-width: unset; color: var(--error);"
                                    onclick="Admin.deleteTaskFromList('${task.task_id}', this)" title="Delete">
                                ✕
                            </button>
                        </div>
                    </td>
                </tr>
            `;
        }).join('');
    },
    
    async restartTaskFromList(taskId, btnEl) {
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

            alert(`Task restarted: ${taskId}`);
            await this.loadAllTasks();
            await this.loadStats();
        } catch (e) {
            console.error('Restart task error:', e);
            alert('Failed to restart task');
        } finally {
            if (btnEl) btnEl.disabled = false;
        }
    },

    async deleteTaskFromList(taskId, btnEl) {
        if (!taskId) return;
        const ok = confirm(`Delete task ${taskId}?`);
        if (!ok) return;

        if (btnEl) btnEl.disabled = true;
        try {
            const resp = await fetch(`/api/admin/task/${taskId}`, { method: 'DELETE' });

            if (!resp.ok) {
                const data = await resp.json().catch(() => ({}));
                alert(data.detail || 'Failed to delete task');
                return;
            }

            await this.loadAllTasks();
            await this.loadStats();
        } catch (e) {
            console.error('Delete task error:', e);
            alert('Failed to delete task');
        } finally {
            if (btnEl) btnEl.disabled = false;
        }
    },
    
    updateAllTasksPagination() {
        const totalPages = Math.ceil(this.allTasksTotal / this.allTasksPerPage);
        const start = this.allTasksTotal > 0 ? (this.allTasksCurrentPage - 1) * this.allTasksPerPage + 1 : 0;
        const end = Math.min(this.allTasksCurrentPage * this.allTasksPerPage, this.allTasksTotal);
        
        document.getElementById('all-tasks-pagination-info').textContent = 
            this.allTasksTotal > 0 ? `Showing ${start}-${end} of ${this.allTasksTotal} tasks` : 'No tasks';
        
        document.getElementById('all-tasks-prev-page').disabled = this.allTasksCurrentPage <= 1;
        document.getElementById('all-tasks-next-page').disabled = this.allTasksCurrentPage >= totalPages;
    },

    escapeHtml(str) {
        return String(str || '')
            .replaceAll('&', '&amp;')
            .replaceAll('<', '&lt;')
            .replaceAll('>', '&gt;')
            .replaceAll('"', '&quot;')
            .replaceAll("'", '&#039;');
    },

    formatPct(value) {
        const n = Number(value);
        if (!Number.isFinite(n)) return '0%';
        return `${n.toFixed(1)}%`;
    }
};

document.addEventListener('DOMContentLoaded', () => Admin.init());
