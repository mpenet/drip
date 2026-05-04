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
  "var _themeOrder = ['light', 'dark', 'system'];
  var _themeLabels = {'light': '☾ Dark', 'dark': '⊙ System', 'system': '☀ Light'};
  function _currentThemePref() {
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
  function _updateLabel(pref) {
    var btn = document.getElementById('theme-btn');
    if (btn) btn.textContent = _themeLabels[pref];
  }
  function toggleTheme() {
    var cur = _currentThemePref();
    var next = _themeOrder[(_themeOrder.indexOf(cur) + 1) % _themeOrder.length];
    if (next === 'system') {
      localStorage.removeItem('drip-theme');
    } else {
      localStorage.setItem('drip-theme', next);
    }
    _applyTheme(next);
    _updateLabel(next);
  }
  (function() { _updateLabel(_currentThemePref()); })();")

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
       [:button {:id "theme-btn" :class "theme-toggle" :onclick "toggleTheme()"} "☾ Dark"]]
      [:script (h/raw theme-toggle-script)]
      (into [:main {:class "container"}]
            (if (sequential? content) content [content]))]]]))
