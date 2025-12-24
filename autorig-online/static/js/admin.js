/**
 * AutoRig Online - Admin Panel
 */

const Admin = {
    currentPage: 1,
    perPage: 20,
    totalUsers: 0,
    sortBy: 'created_at',
    sortDesc: true,
    searchQuery: '',
    selectedUser: null,
    tasksCurrentPage: 1,
    tasksPerPage: 20,
    tasksTotal: 0,
    selectedTasksUser: null,
    
    async init() {
        // Init i18n
        await I18n.init();
        
        // Setup theme
        this.setupTheme();
        
        // Load users
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

        // Search
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
        
        // Sort headers
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
        
        // Pagination
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
            this.updateStats(data);
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
    
    updateStats(data) {
        document.getElementById('stat-users').textContent = data.total;
        
        // Calculate totals from visible data (simplified)
        let totalTasks = 0;
        let totalCredits = 0;
        data.users.forEach(u => {
            totalTasks += u.total_tasks;
            totalCredits += u.balance_credits;
        });
        
        // For accurate stats, we'd need a separate API endpoint
        // For now, show approximate based on current page
        document.getElementById('stat-tasks').textContent = totalTasks + '+';
        document.getElementById('stat-credits').textContent = totalCredits + '+';
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

