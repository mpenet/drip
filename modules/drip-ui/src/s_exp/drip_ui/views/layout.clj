(ns s-exp.drip-ui.views.layout
  (:require [dev.onionpancakes.chassis.core :as h]))

(def ^:private theme-script
  "// Restore theme before first paint to avoid flash
  (function() {
    var t = localStorage.getItem('drip-theme');
    if (t === 'dark') {
      document.documentElement.setAttribute('data-theme', 'dark');
    } else if (t === 'light') {
      document.documentElement.setAttribute('data-theme', 'light');
    } else if (window.matchMedia('(prefers-color-scheme: dark)').matches) {
      document.documentElement.setAttribute('data-theme', 'dark');
    }
  })();")

(def ^:private theme-toggle-script
  "function _currentThemePref() {
    return localStorage.getItem('drip-theme') || 'system';
  }
  function _applyTheme(pref) {
    var html = document.documentElement;
    if (pref === 'system') {
      html.removeAttribute('data-theme');
      if (window.matchMedia('(prefers-color-scheme: dark)').matches) {
        html.setAttribute('data-theme', 'dark');
      } else {
        html.setAttribute('data-theme', 'light');
      }
    } else {
      html.setAttribute('data-theme', pref);
    }
  }
  function _updateActive(pref) {
    document.querySelectorAll('.theme-option').forEach(function(el) {
      el.classList.toggle('active', el.dataset.theme === pref);
    });
  }
  function setTheme(pref) {
    if (pref === 'system') {
      localStorage.removeItem('drip-theme');
    } else {
      localStorage.setItem('drip-theme', pref);
    }
    _applyTheme(pref);
    _updateActive(pref);
    var details = document.getElementById('theme-picker');
    if (details) details.removeAttribute('open');
  }
  (function() { _updateActive(_currentThemePref()); })();")

(defn page
  "Wraps content in the full HTML page shell.
   `active-tab` is :jobs or :queues.
   `title` is the page title suffix.
   `content` is a hiccup form."
  [active-tab title content]
  (h/html
   [h/doctype-html5
    [:html {:lang "en"}
     [:head
      [:meta {:charset "UTF-8"}]
      [:meta {:name "viewport" :content "width=device-width, initial-scale=1.0"}]
      [:title (if title (str "Drip UI – " title) "Drip UI")]
      [:script (h/raw theme-script)]
      [:link {:rel "stylesheet" :href "/public/style.css"}]
      [:script {:type "module" :src "/public/datastar.js"}]]
     [:body
      [:nav {:class "navbar"}
       [:a {:href "/" :class "navbar-brand"}
        [:img {:src "/public/logo.png" :alt "Drip" :class "navbar-logo"}]]
       [:div {:class "nav-tabs"}
        [:a {:href "/jobs" :class (str "nav-tab" (when (= active-tab :jobs) " active"))} "Jobs"]
        [:a {:href "/queues" :class (str "nav-tab" (when (= active-tab :queues) " active"))} "Queues"]]
       [:details {:id "theme-picker" :class "theme-picker"}
        [:summary {:class "theme-toggle" :title "Theme"} "◑"]
        [:div {:class "theme-menu"}
         [:button {:class "theme-option" :data-theme "light" :onclick "setTheme('light')"} "☀ Light"]
         [:button {:class "theme-option" :data-theme "dark" :onclick "setTheme('dark')"} "☾ Dark"]
         [:button {:class "theme-option" :data-theme "system" :onclick "setTheme('system')"} "⊙ System"]]]]
      [:script (h/raw theme-toggle-script)]
      (into [:main {:class "container"}]
            (if (sequential? content) content [content]))]]]))
