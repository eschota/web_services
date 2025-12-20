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
            <tr>
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
                            onclick="Admin.openBalanceModal(${user.id}, '${user.email}', ${user.balance_credits})">
                        Edit Balance
                    </button>
                </td>
            </tr>
        `).join('');
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
    }
};

document.addEventListener('DOMContentLoaded', () => Admin.init());

