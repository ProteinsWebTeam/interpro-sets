window.addEventListener('load', () => {
    fetch('/api/databases')
        .then(response => response.json())
        .then(databases => {

            const promises = [];
            databases.forEach((database, i) => {

                promises.push(
                    fetch('/api/database/' + database.id + '/')
                        .then(response => response.json())
                        .then(sets => {
                            let html = '<h3 class="header">'+ database.name +'<span class="subheader">'+ sets.length.toLocaleString() +' sets</span></h3>'
                                + '<div class="collection">';

                            sets.forEach(set => {
                                html += '<a href="/set/'+ set.accession +'/" class="collection-item"><span class="badge">'+ set.count +'</span>'+ set.accession +'</a>';
                            });

                            html += '</div>';

                            return html;
                        })
                );
            });

            Promise.all(promises).then(values => {
                const className = 'col s' + Math.floor(12 / values.length);
                let html = '';
                values.forEach(col => {
                    html += '<div class="'+ className +'">' + col + '</div>';
                });

                const loader = document.querySelector('.preloader');
                loader.parentNode.removeChild(loader);
                document.getElementById('databases').innerHTML = html;
            });
        });
});