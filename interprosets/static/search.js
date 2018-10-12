window.addEventListener('load', () => {
    document.getElementById('search').addEventListener('keyup', e => {
        if (e.which === 13) {
            const form = document.createElement('form');
            form.action = '/set/' + e.target.value.trim();
            form.method = 'get';
            document.body.appendChild(form);
            form.submit();
        }
    });
});
