function n(n, r) {
    var t = (65535 & n) + (65535 & r);
    return (n >> 16) + (r >> 16) + (t >> 16) << 16 | 65535 & t;
}

function r(r, t, e, o, u, c) {
    return n((f = n(n(t, r), n(o, c))) << (i = u) | f >>> 32 - i, e);
    var f, i;
}

function t(n, t, e, o, u, c, f) {
    return r(t & e | ~t & o, n, t, u, c, f);
}

function e(n, t, e, o, u, c, f) {
    return r(t & o | e & ~o, n, t, u, c, f);
}

function o(n, t, e, o, u, c, f) {
    return r(t ^ e ^ o, n, t, u, c, f);
}

function u(n, t, e, o, u, c, f) {
    return r(e ^ (t | ~o), n, t, u, c, f);
}

function c(r, c) {
    var f, i, a, h, g;
    r[c >> 5] |= 128 << c % 32, r[14 + (c + 64 >>> 9 << 4)] = c;
    var v = 1732584193, l = -271733879, d = -1732584194, C = 271733878;
    for (f = 0; f < r.length; f += 16) i = v, a = l, h = d, g = C, v = t(v, l, d, C, r[f], 7, -680876936), 
    C = t(C, v, l, d, r[f + 1], 12, -389564586), d = t(d, C, v, l, r[f + 2], 17, 606105819), 
    l = t(l, d, C, v, r[f + 3], 22, -1044525330), v = t(v, l, d, C, r[f + 4], 7, -176418897), 
    C = t(C, v, l, d, r[f + 5], 12, 1200080426), d = t(d, C, v, l, r[f + 6], 17, -1473231341), 
    l = t(l, d, C, v, r[f + 7], 22, -45705983), v = t(v, l, d, C, r[f + 8], 7, 1770035416), 
    C = t(C, v, l, d, r[f + 9], 12, -1958414417), d = t(d, C, v, l, r[f + 10], 17, -42063), 
    l = t(l, d, C, v, r[f + 11], 22, -1990404162), v = t(v, l, d, C, r[f + 12], 7, 1804603682), 
    C = t(C, v, l, d, r[f + 13], 12, -40341101), d = t(d, C, v, l, r[f + 14], 17, -1502002290), 
    v = e(v, l = t(l, d, C, v, r[f + 15], 22, 1236535329), d, C, r[f + 1], 5, -165796510), 
    C = e(C, v, l, d, r[f + 6], 9, -1069501632), d = e(d, C, v, l, r[f + 11], 14, 643717713), 
    l = e(l, d, C, v, r[f], 20, -373897302), v = e(v, l, d, C, r[f + 5], 5, -701558691), 
    C = e(C, v, l, d, r[f + 10], 9, 38016083), d = e(d, C, v, l, r[f + 15], 14, -660478335), 
    l = e(l, d, C, v, r[f + 4], 20, -405537848), v = e(v, l, d, C, r[f + 9], 5, 568446438), 
    C = e(C, v, l, d, r[f + 14], 9, -1019803690), d = e(d, C, v, l, r[f + 3], 14, -187363961), 
    l = e(l, d, C, v, r[f + 8], 20, 1163531501), v = e(v, l, d, C, r[f + 13], 5, -1444681467), 
    C = e(C, v, l, d, r[f + 2], 9, -51403784), d = e(d, C, v, l, r[f + 7], 14, 1735328473), 
    v = o(v, l = e(l, d, C, v, r[f + 12], 20, -1926607734), d, C, r[f + 5], 4, -378558), 
    C = o(C, v, l, d, r[f + 8], 11, -2022574463), d = o(d, C, v, l, r[f + 11], 16, 1839030562), 
    l = o(l, d, C, v, r[f + 14], 23, -35309556), v = o(v, l, d, C, r[f + 1], 4, -1530992060), 
    C = o(C, v, l, d, r[f + 4], 11, 1272893353), d = o(d, C, v, l, r[f + 7], 16, -155497632), 
    l = o(l, d, C, v, r[f + 10], 23, -1094730640), v = o(v, l, d, C, r[f + 13], 4, 681279174), 
    C = o(C, v, l, d, r[f], 11, -358537222), d = o(d, C, v, l, r[f + 3], 16, -722521979), 
    l = o(l, d, C, v, r[f + 6], 23, 76029189), v = o(v, l, d, C, r[f + 9], 4, -640364487), 
    C = o(C, v, l, d, r[f + 12], 11, -421815835), d = o(d, C, v, l, r[f + 15], 16, 530742520), 
    v = u(v, l = o(l, d, C, v, r[f + 2], 23, -995338651), d, C, r[f], 6, -198630844), 
    C = u(C, v, l, d, r[f + 7], 10, 1126891415), d = u(d, C, v, l, r[f + 14], 15, -1416354905), 
    l = u(l, d, C, v, r[f + 5], 21, -57434055), v = u(v, l, d, C, r[f + 12], 6, 1700485571), 
    C = u(C, v, l, d, r[f + 3], 10, -1894986606), d = u(d, C, v, l, r[f + 10], 15, -1051523), 
    l = u(l, d, C, v, r[f + 1], 21, -2054922799), v = u(v, l, d, C, r[f + 8], 6, 1873313359), 
    C = u(C, v, l, d, r[f + 15], 10, -30611744), d = u(d, C, v, l, r[f + 6], 15, -1560198380), 
    l = u(l, d, C, v, r[f + 13], 21, 1309151649), v = u(v, l, d, C, r[f + 4], 6, -145523070), 
    C = u(C, v, l, d, r[f + 11], 10, -1120210379), d = u(d, C, v, l, r[f + 2], 15, 718787259), 
    l = u(l, d, C, v, r[f + 9], 21, -343485551), v = n(v, i), l = n(l, a), d = n(d, h), 
    C = n(C, g);
    return [ v, l, d, C ];
}

function f(n) {
    var r, t = "", e = 32 * n.length;
    for (r = 0; r < e; r += 8) t += String.fromCharCode(n[r >> 5] >>> r % 32 & 255);
    return t;
}

function i(n) {
    var r, t = [];
    for (t[(n.length >> 2) - 1] = void 0, r = 0; r < t.length; r += 1) t[r] = 0;
    var e = 8 * n.length;
    for (r = 0; r < e; r += 8) t[r >> 5] |= (255 & n.charCodeAt(r / 8)) << r % 32;
    return t;
}

function a(n) {
    var r, t, e = "0123456789abcdef", o = "";
    for (t = 0; t < n.length; t += 1) r = n.charCodeAt(t), o += e.charAt(r >>> 4 & 15) + e.charAt(15 & r);
    return o;
}

function h(n) {
    return unescape(encodeURIComponent(n));
}

function g(n) {
    return function(n) {
        return f(c(i(n), 8 * n.length));
    }(h(n));
}

function v(n, r) {
    return function(n, r) {
        var t, e, o = i(n), u = [], a = [];
        for (u[15] = a[15] = void 0, o.length > 16 && (o = c(o, 8 * n.length)), t = 0; t < 16; t += 1) u[t] = 909522486 ^ o[t], 
        a[t] = 1549556828 ^ o[t];
        return e = c(u.concat(i(r)), 512 + 8 * r.length), f(c(a.concat(e), 640));
    }(h(n), h(r));
}

exports.md5 = function(n, r, t) {
    return r ? t ? v(r, n) : a(v(r, n)) : t ? g(n) : a(g(n));
};