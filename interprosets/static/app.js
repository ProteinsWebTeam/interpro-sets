function queryAPI(url, accession) {
    const errorCard = document.getElementById('error');
    errorCard.style.display = 'none';

    return fetch(url)
        .then((response) => {
            if (response.ok) return response.json();
            throw new Error('Could not get results for <strong>'+ accession + '</strong>.');
        })
        .catch(error => {
            error.name = '';
            errorCard.querySelector('p').innerHTML = error.toString();
            errorCard.style.display = 'block';
        });
}

function deepCopy(obj) {
    return JSON.parse(JSON.stringify(obj));
}

const network = {
    root: null,
    svg: null,
    width: null,
    height: null,
    margin: null,
    data: null,
    simulation: null,
    threshold: null,
    mode: 'network',
    init: function (selector) {
        this.width = selector.clientWidth;
        this.height = this.width * 0.6;
        this.root = selector;
        this.svg = d3.select(selector)
            .append('svg')
            .attr('width', this.width)
            .attr('height', this.height);
        this.svg.append('g').attr('class', 'nodes');
        this.svg.append('g').attr('class', 'edges');
    },
    reset: function () {
        if (this.simulation) {
            this.simulation.stop();
            this.simulation = null;
        }

        this.svg.remove();
        this.svg = d3.select(this.root)
            .append('svg')
            .attr('width', this.width)
            .attr('height', this.height);
        this.svg.append('g').attr('class', 'nodes');
        this.svg.append('g').attr('class', 'edges');
    },
    setMode: function (mode) {
        this.mode = mode;
        this.reset();
    },
    update: function () {
        if (this.mode === 'chord') {
            this.updateRadial();
            return;
        } else if (this.mode === 'tree') {
            this.updateTree();
            return;
        }

        const nodes = deepCopy(this.data.nodes);
        const links = deepCopy(this.data.links)
            .filter(x => this.threshold === null || x.value < this.threshold);
        const radius = 10;
        const size = Math.min(this.width, this.height);

        links.forEach(x => {
            if (x.value < Number.MIN_VALUE)
                x.value = Number.MIN_VALUE;
        });

        const scale = d3.scaleLog()
            .base(10)
            .domain([d3.min(links, d => d.value), d3.max(links, d => d.value)])
            .range([50, size / 2]);

        let node = this.svg.select('.nodes').selectAll('.node').data(nodes);
        node.exit().remove();

        const _node = node.enter()
            .append('g')
            .attr('class', 'node')
            .style('transform-origin', '50% 50%');

        _node.append('circle')
            .attr('r', radius)
            .attr('fill', '#00a99d');

        _node.append('text')
            .attr('dx', radius)
            .style('text-anchor', 'start')
            .style('dominant-baseline', 'middle')
            .text(d => nvl(d.name, d.accession));

        node = _node.merge(node);
        node.call(d3.drag()
            .on('start', dragstarted)
            .on('drag', dragged)
            .on('end', dragended));

        let edge = this.svg.select('.edges').selectAll('path').data(links);
        let label = this.svg.select('.edges').selectAll('text').data(links);
        edge.exit().remove();
        label.exit().remove();

        edge = edge.enter()
            .append('path')
            .attr('stroke', '#ccc')
            .attr('fill', 'none')
            .merge(edge);

        label = label.enter()
            .append('text')
            .text(d => d.value.toExponential(1))
            .attr('stroke', '#cccccc')
            .merge(label);

        function ticked() {
            node.attr('transform', d => 'translate(' + d.x + ',' + d.y + ')');

            edge.attr('d', d => {
                return 'M' +
                    d.source.x + ' ' +
                    d.source.y + ' L ' +
                    d.target.x + ' ' +
                    d.target.y;
            });

            label.attr('transform', d => {
                const x = (d.source.x + d.target.x) / 2;
                const y = (d.source.y + d.target.y) / 2;
                return 'translate(' + x + ',' + y + ')';
            });
        }

        const self = this;
        if (this.simulation === null) {
            this.simulation = d3.forceSimulation()
                .force('charge', d3.forceManyBody().strength(-5))
                .force('center', d3.forceCenter(this.width / 2, this.height / 2))
                .force('collision', d3.forceCollide(radius*2))
                .force('link', d3.forceLink().id(d => d.accession))
                .stop();
        }

        this.simulation.nodes(nodes);
        this.simulation.force('link')
            .links(links)
            .distance(d => scale(d.value));
        this.simulation.alpha(1).on('tick', ticked).restart();

        function dragstarted(d) {
            if (!d3.event.active) self.simulation.alphaTarget(0.3).restart();
            d.fx = d.x;
            d.fy = d.y;
        }

        function dragged(d) {
            d.fx = d3.event.x;
            d.fy = d3.event.y;
        }

        function dragended(d) {
            if (!d3.event.active) self.simulation.alphaTarget(0);
            d.fx = null;
            d.fy = null;
        }
    },
    updateRadial: function () {
        const nodes = deepCopy(this.data.nodes);
        const links = deepCopy(this.data.links).filter(i => this.threshold === null || i.value < this.threshold);
        const radius = 10;
        const radialRadius = Math.min(this.width, this.height)/3;

        let node = this.svg.select('.nodes')
            .attr('transform', 'translate(' + this.width/2 + ',' + this.height/2 + ')')
            .selectAll('.node').data(nodes);
        node.exit().remove();

        const _node = node.enter()
            .append('g')
            .attr('class', 'node')
            .style('transform-origin', '50% 50%');

        _node.append('circle')
            .attr('r', radius)
            .attr('fill', '#00a99d');

        _node.append('text')
            .attr('dx', radius)
            .style('text-anchor', 'start')
            .style('dominant-baseline', 'middle')
            .text(d => nvl(d.name, d.accession));

        node = _node.merge(node);

        let edge = this.svg.select('.edges')
            .attr('transform', 'translate(' + this.width/2 + ',' + this.height/2 + ')')
            .selectAll('path').data(links);
        let label = this.svg.select('.edges').selectAll('text').data(links);
        edge.exit().remove();
        label.exit().remove();

        edge = edge.enter()
            .append('path')
            .attr('stroke', '#ccc')
            .attr('fill', 'none')
            .merge(edge);

        // label = label.enter()
        //     .append('text')
        //     .text(d => d.value.toExponential(1))
        //     .attr('stroke', '#cccccc')
        //     .merge(label);

        const self = this;
        let _nodes = Array(nodes.length);
        function ticked() {
            node.attr('transform', (d, i) => {
                _nodes[i] = {x: d.x, y: d.y};
                return 'translate(' + d.x + ',' + d.y + ')';
            });

            edge.attr('d', d => {
                const source = _nodes[nodes.findIndex(e => e.accession === d.source || e.accession === d.source.accession)];
                const target = _nodes[nodes.findIndex(e => e.accession === d.target || e.accession === d.target.accession)];

                let x1, x2, y1, y2;
                if (source.x <= target.x) {
                    x1 = source.x;
                    y1 = source.y;
                    x2 = target.x;
                    y2 = target.y;
                } else {
                    x2 = source.x;
                    y2 = source.y;
                    x1 = target.x;
                    y1 = target.y;
                }

                const midX = (source.x + target.x) / 2;
                const midY = (source.y + target.y) / 2;
                let sweepFlag;
                if (midX <= 0) {
                    if (midY <= 0)
                        sweepFlag = 0;
                    else
                        sweepFlag = 1;
                } else if (midY <= 0)
                    sweepFlag = 0;
                else
                    sweepFlag = 1;

                // if ((d.source === 'PF16346' || d.source === 'PF08244') && (d.target === 'PF16346' || d.target === 'PF08244')) {
                //     console.log('---------');
                //     console.log(d);
                //     console.log(source);
                //     console.log(target);
                //     console.log(midX + ' ' + midY);
                //     console.log(sweepFlag);
                // }

                const dx = target.x - source.x;
                const dy = target.y - source.y;
                const dr = Math.sqrt(dx * dx + dy * dy);

                return 'M ' +
                    x1 + ' ' +
                    y1 + ' A ' +
                    dr + ' ' + dr + ' 0 0 '+ sweepFlag +' ' +
                    x2 + ' ' + y2;
            });
        }

        if (this.simulation === null) {
            this.simulation = d3.forceSimulation()
                .force('collision', d3.forceCollide().radius(radius*2))
                .force('r', d3.forceRadial(radialRadius))
                .stop();
        }

        this.simulation.nodes(nodes);
        this.simulation.on('tick', ticked).alpha(2).restart();


    },
    updateTree: function () {
        const links = deepCopy(this.data.links);
        links.sort((a, b) => a.value - b.value);

        function findRoot(nodes, key, debug) {
            let parentKey = key;

            while (nodes.hasOwnProperty(parentKey) && nodes[parentKey] !== null) {
                key = parentKey;
                parentKey = nodes[parentKey];
            }

            return parentKey;
        }

        const parentsOf = {};
        let nodes = {};
        this.data.nodes.forEach(n => {
            nodes[n.accession] = {name: nvl(n.name, n.accession)};
            parentsOf[n.accession] = null;
        });

        let k;
        for (let i = 0; i < links.length; i++) {
            let k1 = links[i].source;
            let k2 = links[i].target;

            if (!nodes.hasOwnProperty(k1) || !nodes.hasOwnProperty(k2))
                continue;

            if (parentsOf.hasOwnProperty(k1))
                k1 = findRoot(parentsOf, k1);

            if (parentsOf.hasOwnProperty(k2))
                k2 = findRoot(parentsOf, k2);

            if (k1 !== k2) {
                k = i;
                parentsOf[k1] = k;
                parentsOf[k2] = k;
                parentsOf[k] = null;
                nodes[k] = {
                    name: k,
                    children: [nodes[k1], nodes[k2]]
                };
            }

        }

        const branches = [];
        for (let key in parentsOf) {
            if (parentsOf.hasOwnProperty(key) && parentsOf[key] === null)
                branches.push(key);
        }

        if (branches.length === 1)
            nodes = nodes[branches[0]];
        else
            nodes = {
                name: 'root',
                children: branches.map(key => nodes[key])
            };

        const outerRadius = Math.min(this.width, this.height) / 2;
        const innerRadius = outerRadius - 100;

        const cluster = d3.cluster()
            .size([360, innerRadius])
            .separation(d => 1);

        const root = d3.hierarchy(nodes);

        cluster(root);
        this.svg.select('.edges')
            .attr('transform', 'translate(' + this.width/2 + ',' + this.height/2 + ')')
            .attr('fill', 'none')
            .attr('stroke', '#ccc')
            .selectAll('path')
            .data(root.links())
            .enter().append('path')
            .attr('d', d => {
                const startAngle = (d.source.x - 90) / 180 * Math.PI;
                const startRadius = d.source.y;
                const endAngle = (d.target.x - 90) / 180 * Math.PI;
                const endRadius = d.target.y;

                const c0 = Math.cos(startAngle);
                const  s0 = Math.sin(startAngle);
                const c1 = Math.cos(endAngle);
                const s1 = Math.sin(endAngle);
                return "M" + startRadius * c0 + "," + startRadius * s0
                    + (endAngle === startAngle ? "" : "A" + startRadius + "," + startRadius + " 0 0 " + (endAngle > startAngle ? 1 : 0) + " " + startRadius * c1 + "," + startRadius * s1)
                    + "L" + endRadius * c1 + "," + endRadius * s1;
            });

        this.svg.select('.nodes')
            .attr('transform', 'translate(' + this.width/2 + ',' + this.height/2 + ')')
            .selectAll('text')
            .data(root.leaves())
            .enter().append('text')
            .attr('transform', d => 'rotate('+ (d.x - 90) +') translate(' + innerRadius + ',0) ' + (d.x < 180 ? '': 'rotate(180)'))
            .attr('text-anchor', d => d.x < 180 ? 'start' : 'end')
            .text(d => d.data.name);
    }
};


function getRelationships(accession) {
    queryAPI('/api/set/' + accession + '/relationships/')
        .then(response => {
            network.data = response.data;
            network.update();
        });
}

function getTargets(accession) {
    queryAPI('/api/entry/' + accession + '/targets/', accession)
        .then(entry => {
            const div = document.getElementById('hits');
            const domainColors = [
                // current set
                ["#81C784", "#4CAF50", "#388E3C"],
                // no set
                ["#64B5F6", "#2196F3", "#1976D2"],
                // other set
                ["#e57373", "#f44336", "#d32f2f"]
            ];
            let style = getComputedStyle(div, null);
            let padding = parseFloat(style.getPropertyValue('padding-left'))
                + parseFloat(style.getPropertyValue('padding-right'));
            let svgWidth = div.clientWidth - padding;
            const svgLeftPadding = 100;
            let scale = d3.scaleLinear()
                .domain([0, entry.sequence.length])
                .range([5, svgWidth-svgLeftPadding-5]);
            const evalueScale = d3.scaleThreshold()
                .domain([1e-6, 1e-3])
                .range([2, 1, 0]);

            let html = '<h4>'+ nvl(entry.name, entry.accession)
                + '<div class="subheader">'
                + entry.accession +'</div>'
                + '</h4>'
                + '<pre class="wrap">'+ entry.sequence +'</pre>'
            + '<svg width="'+ svgWidth +'" height="'+ (entry.targets.length + 1) * 20 +'">' +
                '<g class="domains" transform="translate('+svgLeftPadding+')"></g>' +
                '<g class="labels"></g>' +
                '</svg>';
            let svgDomains = '';
            let svgLabels = '';

            entry.targets.forEach((target, i) => {
                let set;
                let colors;

                if (target.set === entry.set) {
                    set = '<span class="green badge label"><a href="/set/'+ target.set +'">'+ target.set +'</a></span>';
                    colors = domainColors[0];
                } else if (target.set === null) {
                    set = '<span class="blue badge label">N/A</span>';
                    colors = domainColors[1];
                } else {
                    set = '<span class="red badge label"><a href="/set/'+ target.set +'">'+ target.set +'</a></span>';
                    colors = domainColors[2];
                }

                html += '<div class="card-panel">' +
                    '<div class="row">' +
                    '<div class="col s6 valign-wrapper">' +
                    '<h5 class="header">'+ nvl(target.name, target.accession) +'<span class="subheader">'+ target.accession +'</span></h5>' +
                    '</div>' +
                    '<div class="col s2 valign-wrapper">' +
                    '<div class="statistic"><span class="label">Set</span>'+ set +' </div>' +
                    '</div>' +
                    '<div class="col s2 valign-wrapper">' +
                    '<div class="statistic"><span class="label">E-value</span>'+ target.evalue.toExponential() +' </div>' +
                    '</div>' +
                    '<div class="col s2 valign-wrapper">' +
                    '<div class="statistic"><span class="label">Domains</span>'+ target.domains.length +' </div>' +
                    '</div>' +
                    '</div>' +
                '<svg data-index="'+ i +'"></svg>';

                svgLabels += '<text x="'+ (svgLeftPadding - 5) +'" y="'+ (20*i + 25) +'">'+ nvl(target.name, target.accession) +'</text>';
                svgDomains += '<line x1="0" y1="'+(5+20*(i+1))+'" x2="'+svgWidth+'" y2="'+(5+20*(i+1))+'" stroke="#d7d7d7" />';

                target.domains.forEach(domain => {
                    html += '<div class="row">'
                        + '<div class="col s2 valign-wrapper">'
                        + '<div class="statistic"><span class="label">i-Evalue</span>'+ domain.ievalue.toExponential() +' </div>'
                        + '</div>'
                        + '<div class="col s10 valign-wrapper">'
                        + '<pre>'+ domain.query + '<br>' + domain.target + '</pre>'
                        + '</div>'
                        + '</div>';

                    const color = colors[evalueScale(domain.ievalue)];
                    const x = scale(domain.start-1);
                    const w = scale(domain.end) - x;
                    svgDomains += '<g class="domain" transform="translate('+x+','+ (20 * i) +')">' +
                        '<rect x="0" y="20" width="'+ w +'" height="10" fill="'+ color +'" />' +
                        '<text x="0" y="18" text-anchor="end">'+ domain.start +'</text>' +
                        '<text x="'+ w +'" y="18">'+ domain.end +'</text>' +
                        '</g>';
                });

                html += '</div>';
            });

            div.innerHTML = html;
            div.querySelector('svg .domains').innerHTML = svgDomains;
            div.querySelector('svg .labels').innerHTML = svgLabels;

            const card = div.querySelector('.card-panel');
            style = getComputedStyle(card, null);
            padding = parseFloat(style.getPropertyValue('padding-left'))
                + parseFloat(style.getPropertyValue('padding-right'));
            svgWidth = card.clientWidth - padding;
            scale = d3.scaleLinear()
                .domain([0, entry.sequence.length])
                .range([5, svgWidth-5]);

            // Now create SVG
            entry.targets.forEach((target, i) => {
                let colors;
                if (target.set === entry.set)
                    colors = domainColors[0];
                else if (target.set === null)
                    colors = domainColors[1];
                else
                    colors = domainColors[2];

                const svg = div.querySelector('[data-index="'+ i +'"]');
                svg.setAttribute('width', svgWidth);
                svg.setAttribute('height', 50);

                let content = '<line x1="0" y1="25" x2="'+svgWidth+'" y2="25" stroke="#d7d7d7" />';
                target.domains.forEach((domain, j) => {
                    const color = colors[evalueScale(domain.ievalue)];
                    const x = scale(domain.start-1);
                    const w = scale(domain.end) - x;
                    content += '<g class="domain" transform="translate('+x+')">' +
                        '<rect x="0" y="20" width="'+ w +'" height="10" fill="'+ color +'" />' +
                        '<text x="0" y="18" text-anchor="end">'+ domain.start +'</text>' +
                        '<text x="'+ w +'" y="18">'+ domain.end +'</text>' +
                        '</g>';
                });

                svg.innerHTML = content;
            });


            Array.from(div.querySelectorAll('.valign-wrapper')).forEach(elem => {
                elem.style.height = elem.parentNode.offsetHeight.toString() + 'px';
            });
        });
}


function getSetMembers(accession) {
    queryAPI('/api/set/' + accession + '/', accession)
        .then(members => {
            let html = '';
            members.forEach((member, i) => {
                html += '<a href="#!" data-accession="'+ member.accession +'" class="collection-item '+ (i ? '' : 'active') +'">';

                if (member.targets_other_set)
                    html += '<span class="badge red-text text-darken-2"><i class="material-icons">error_outline</i>&nbsp;' + member.targets + '</span>';
                else if (member.targets_without_set)
                    html += '<span class="badge"><i class="material-icons">error_outline</i>&nbsp;' + member.targets + '</span>';
                else
                    html += '<span class="badge">' + member.targets + '</span>';

                html += nvl(member.name, member.accession) + '</a>';
            });


            const collection = document.getElementById('members');
            collection.innerHTML = html;

            Array.from(collection.querySelectorAll('.collection-item')).forEach(item => {
                item.addEventListener('click', e => {
                    const activeItem = collection.querySelector('.collection-item.active');
                    if (activeItem) activeItem.className = 'collection-item';
                    item.className = 'collection-item active';
                    getTargets(item.getAttribute('data-accession'));
                });
            });

            getTargets(members[0].accession);
        });
}


function nvl(value, fallback) {
    return value || fallback;
}


const heatmap = {
    g: null,
    width: null,
    height: null,
    margin: null,
    entries: null,
    data: null,
    threshold: null,
    evalueCutoff: null,
    init: function (selector, margin) {
        this.width = selector.clientWidth;
        this.height = this.width;
        this.margin = margin;
        this.g = d3.select(selector)
            .append('svg')
            .attr('width', this.width)
            .attr('height', this.height)
            .append('g')
            .attr('transform', 'translate(' + margin.left +',' + margin.top + ')');
    },
    update: function () {
        const entries = this.entries;
        const g = this.g;
        const width = this.width;
        const margin = this.margin;
        const itemSize = Math.min(Math.floor((width - margin.left) / entries.length), 30);
        const data = this.data;
        const cutoff = this.evalueCutoff;

        const band = d3.scaleBand()
            .domain(entries.map((item, i) => nvl(item.name, item.accession)))
            .range([0, entries.length * itemSize]);

        const deg = -65;
        const adj = Math.cos(Math.abs(deg * Math.PI / 180)) * itemSize;
        const opp = Math.sin(Math.abs(deg * Math.PI / 180)) * itemSize;

        g.select('g.x.axis').remove();
        g.append('g')
            .attr('class', 'x axis')
            .style('text-anchor', 'start')
            .call(d3.axisTop(band).tickSize(0))
            .selectAll('text')
            .attr('dx', () => Math.floor(opp / 3) + 'px' )
            .attr('dy', () => Math.floor(adj / 3) + 'px' )
            .attr('transform', () => 'rotate('+ deg +')' );

        g.select('g.y.axis').remove();
        g.append('g')
            .attr('class', 'y axis')
            .style('text-anchor', 'end')
            .call(d3.axisLeft(band).tickSize(0));

        // Join data
        const rect = g.selectAll('rect')
            .data(data);

        // Create new rects
        rect.enter()
            .append('rect')
            .attr('x', (d, i) => (i % entries.length) * itemSize )
            .attr('y', (d, i) => Math.floor(i / entries.length) * itemSize )
            .attr('width', itemSize)
            .attr('height', itemSize)
            .style('fill', '#e5f5e0')
            .merge(rect)  // update new AND existing
            .transition()
            .duration(500)
            .style('fill', d => {
                if (d === null)
                    return '#e5f5e0';
                else if (cutoff !== null && d > cutoff)
                    return '#a1d99b';
                else
                    return '#31a354';
            });

        rect.exit().remove();
    }
};


function getSimilarities(accession) {
    const url = '/api/set/' + accession + '/similarity/';
    const div = document.getElementById('heatmap');

    queryAPI(url, accession)
        .then(response => {
            heatmap.entries = response.methods;
            heatmap.data = (function () {
                const data = [];
                response.data.forEach(row => {
                    row.forEach(cell => {
                        data.push(cell);
                    })
                });
                return data;
            })();

            heatmap.update();
        });
}

window.addEventListener('load', () => {
    const accession = window.location.pathname.match(/^\/set\/(.+?)\/?$/i)[1];
    document.title = accession + ' | InterPro';
    document.getElementById('search').value = accession;
    document.querySelector('label[for=search]').className = 'active';
    document.getElementById('accession').innerText = accession;

    network.init(document.querySelector('#network .chart'));
    heatmap.init(document.querySelector('#heatmap .chart'), {left: 100, top: 100});

    (function () {
        const input = document.getElementById('sim-input');
        input.addEventListener('keyup', e => {
            if (e.which !== 13) return;
            const val = parseFloat(e.target.value);
            if (Number.isNaN())
                e.target.className = 'invalid';
            else {
                heatmap.evalueCutoff = val;
                heatmap.update();
            }
        });

        heatmap.evalueCutoff = input.value.length ? parseFloat(input.value) : null;
    })();

    (function () {
        const input = document.getElementById('rel-threshold');
        input.addEventListener('keyup', e => {
            if (e.which !== 13) return;
            let val = e.target.value;

            if (val.length) {
                val = parseFloat(e.target.value);
                if (Number.isNaN(val)) {
                    e.target.className = 'invalid';
                    return;
                }
            } else
                val = null;

            network.threshold = val;
            network.update();
        });
    })();

    // Init tabs
    let currentTab = null;

    Array.from(document.querySelectorAll('input[name=similarity]')).forEach(input => {
        input.addEventListener('change', e => {
            getSimilarities(accession);
        });
    });

    Array.from(document.querySelectorAll('input[name=vis-type]')).forEach(input => {
        input.addEventListener('change', e => {
            network.setMode(e.target.value);
            network.update();
        });
    });

    function showTab(tab) {
        if (tab.id === 'hmmscan')
            getSetMembers(accession);
        else if (tab.id === 'network')
            getRelationships(accession);
        else if (tab.id === 'heatmap')
            getSimilarities(accession);
        else
            console.error(tab.id);
    }

    M.Tabs.init(document.querySelector('.tabs'), {
        onShow: function (tab) {
            if (tab === currentTab) return;
            currentTab = tab;
            showTab(currentTab);
        }
    });

    currentTab = document.querySelector('section.active');
    showTab(currentTab);
});
