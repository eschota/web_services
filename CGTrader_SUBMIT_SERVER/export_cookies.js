// Скрипт для экспорта cookies из браузера
// Запустите в консоли браузера (F12 -> Console) на странице cgtrader.com

(function() {
    // Получаем все cookies для текущего домена
    const cookies = document.cookie.split(';').reduce((acc, cookie) => {
        const [name, value] = cookie.trim().split('=');
        if (name && value) {
            acc[name] = decodeURIComponent(value);
        }
        return acc;
    }, {});
    
    // Формируем JSON объект
    const cookiesObj = {
        cookies: cookies,
        timestamp: new Date().toISOString(),
        domain: window.location.hostname
    };
    
    // Выводим в консоль
    console.log('=== CGTrader Cookies ===');
    console.log(JSON.stringify(cookiesObj, null, 2));
    
    // Копируем в буфер обмена (если доступно)
    if (navigator.clipboard) {
        navigator.clipboard.writeText(JSON.stringify(cookies, null, 2)).then(() => {
            console.log('✅ Cookies скопированы в буфер обмена!');
        }).catch(err => {
            console.log('⚠️ Не удалось скопировать в буфер, но JSON выше');
        });
    }
    
    // Возвращаем объект
    return cookiesObj;
})();
