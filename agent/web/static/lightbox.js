/* Лайтбокс просмотра фото взвешиваний.
   Делегированный обработчик: любой элемент с классом .js-lightbox
   и атрибутами data-full (URL оригинала) и data-caption (подпись)
   открывает фото в оверлее. Закрытие — клик по фону, крестик, Escape.
   Слушатель в фазе захвата, чтобы клик по миниатюре в кликабельной
   строке таблицы не срабатывал как переход по строке. */
(function () {
  'use strict';

  var overlay = null;

  function build() {
    overlay = document.createElement('div');
    overlay.className = 'lightbox';
    overlay.hidden = true;
    overlay.innerHTML =
      '<button type="button" class="lightbox-close" aria-label="Закрыть">✕</button>' +
      '<figure class="lightbox-body">' +
      '<img class="lightbox-img" alt="">' +
      '<figcaption class="lightbox-caption"></figcaption>' +
      '</figure>';
    overlay.addEventListener('click', function (event) {
      if (event.target === overlay || event.target.closest('.lightbox-close')) {
        close();
      }
    });
    document.body.appendChild(overlay);
  }

  function open(trigger) {
    if (!overlay) {
      build();
    }
    var image = overlay.querySelector('.lightbox-img');
    var caption = trigger.getAttribute('data-caption') || '';
    image.src = trigger.getAttribute('data-full');
    image.alt = caption;
    overlay.querySelector('.lightbox-caption').textContent = caption;
    overlay.hidden = false;
    document.body.classList.add('lightbox-open');
  }

  function close() {
    if (!overlay) {
      return;
    }
    overlay.hidden = true;
    overlay.querySelector('.lightbox-img').removeAttribute('src');
    document.body.classList.remove('lightbox-open');
  }

  document.addEventListener(
    'click',
    function (event) {
      var trigger = event.target.closest && event.target.closest('.js-lightbox');
      if (!trigger) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      open(trigger);
    },
    true
  );

  document.addEventListener('keydown', function (event) {
    if (event.key === 'Escape' && overlay && !overlay.hidden) {
      close();
    }
  });
})();
