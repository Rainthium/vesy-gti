// Всплывающая подсказка графиков «Отчётов»: элементы SVG с атрибутом data-tip
// (столбики, точки, невидимые зоны попадания — center/web/charts.py) показывают
// плашку у курсора сразу, без задержки стандартного <title>. Без библиотек.
(function () {
  'use strict';
  var tip = null;
  var current = null;

  function ensure() {
    if (!tip) {
      tip = document.createElement('div');
      tip.className = 'chart-tip';
      tip.hidden = true;
      document.body.appendChild(tip);
    }
    return tip;
  }

  function target(ev) {
    var el = ev.target;
    return el && el.closest ? el.closest('[data-tip]') : null;
  }

  function place(ev) {
    var box = ensure();
    var x = ev.pageX + 14;
    var y = ev.pageY + 14;
    var maxX = window.scrollX + document.documentElement.clientWidth - 8;
    var maxY = window.scrollY + document.documentElement.clientHeight - 8;
    if (x + box.offsetWidth > maxX) x = ev.pageX - box.offsetWidth - 14;
    if (y + box.offsetHeight > maxY) y = ev.pageY - box.offsetHeight - 14;
    box.style.left = x + 'px';
    box.style.top = y + 'px';
  }

  function show(el, ev) {
    var box = ensure();
    current = el;
    box.textContent = el.getAttribute('data-tip') || '';
    box.hidden = false;
    place(ev);
  }

  function hide() {
    current = null;
    if (tip) tip.hidden = true;
  }

  document.addEventListener('mousemove', function (ev) {
    var el = target(ev);
    if (!el) {
      if (current) hide();
      return;
    }
    if (el !== current) show(el, ev);
    else place(ev);
  });
  document.addEventListener('mouseleave', hide);
  // прокрутка и печать: плашка у старого места не нужна
  window.addEventListener('scroll', hide, { passive: true });
  window.addEventListener('beforeprint', hide);
})();
