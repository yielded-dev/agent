/// <reference lib="dom" />

/* @license
 * Selection and accessible naming adapted from jev-ultrafast snapshot.js at 1231850.
 * Copyright (c) 2026 Browser Use
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

import type { Control } from "@yielded/agent/browser-use";

/** Isolated-realm Jev observation. The same private refs and native guards own input. */
export const inspectJevDom = (
  selector: string | undefined,
  prefix: string,
  maximum: number,
  optionFilter: string | undefined,
) => {
  const registry = new Map<string, Element>();

  const checks = new Map<
    string,
    { name: string; originalName: string; kind: string; viewport: boolean; verify: () => boolean }
  >();

  Reflect.set(globalThis, "@effect-agent/native-browser", registry);
  Reflect.set(globalThis, "@effect-agent/native-browser-name-checks", checks);

  const visible = (node: Element) =>
    node.closest('[aria-hidden="true"],[inert]') === null &&
    node.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true });

  const inView = (node: Element) => {
    const rect = node.getBoundingClientRect();
    const x = rect.x + rect.width / 2;
    const y = rect.y + rect.height / 2;

    return (
      rect.width > 0 && rect.height > 0 && x >= 0 && y >= 0 && x < innerWidth && y < innerHeight
    );
  };

  const name = (node: Element | null, seen = new Set<Element>()): string => {
    if (node === null || seen.has(node) || seen.size >= 10_000) return "";
    seen.add(node);

    const referenced = (node.getAttribute("aria-labelledby") ?? "")
      .split(/\s+/)
      .map((id) => name(document.getElementById(id), seen))
      .filter(Boolean)
      .join(" ");

    const labels =
      node instanceof HTMLInputElement ||
      node instanceof HTMLTextAreaElement ||
      node instanceof HTMLSelectElement
        ? node.labels
        : null;

    return (
      referenced ||
      node.getAttribute("aria-label") ||
      Array.from(labels ?? [])
        .map((label) => name(label, seen))
        .filter(Boolean)
        .join(" ") ||
      (node instanceof HTMLInputElement && ["button", "submit", "reset"].includes(node.type)
        ? node.value
        : "") ||
      node.getAttribute("alt") ||
      (node.tagName === "INPUT"
        ? ""
        : Array.from(node.childNodes)
            .map((child) =>
              child instanceof Text
                ? child.textContent
                : child instanceof Element && child.getAttribute("aria-hidden") !== "true"
                  ? name(child, seen)
                  : "",
            )
            .join(" ")
            .trim()) ||
      node.getAttribute("title") ||
      node.getAttribute("placeholder") ||
      ""
    ).slice(0, 4_096);
  };

  const roles = [
    "button",
    "link",
    "checkbox",
    "radio",
    "switch",
    "tab",
    "menuitem",
    "menuitemradio",
    "option",
    "gridcell",
    "combobox",
    "textbox",
    "searchbox",
    "spinbutton",
  ];

  const role = (node: HTMLElement) => {
    const explicit = node.getAttribute("role");

    if (explicit !== null && roles.includes(explicit)) return explicit;
    if (node.tagName === "BUTTON" || node.tagName === "SUMMARY") return "button";
    if (node.tagName === "A") return "link";
    if (node instanceof HTMLSelectElement) return "combobox";
    if (node instanceof HTMLTextAreaElement || node.isContentEditable) return "textbox";
    if (node instanceof HTMLInputElement) {
      if (["checkbox", "radio"].includes(node.type)) return node.type;
      if (["button", "submit", "reset", "image"].includes(node.type)) return "button";
      if (node.type === "search") return "searchbox";
      if (node.type === "number") return "spinbutton";
      if (["text", "email", "url", "tel"].includes(node.type)) return "textbox";
    }

    return null;
  };

  const value = (node: HTMLElement) =>
    ("value" in node
      ? String(node.value)
      : node.isContentEditable || role(node) === "combobox"
        ? node.innerText.trim()
        : ""
    ).slice(0, 4_096);

  const readOnly = (node: HTMLElement) =>
    ((node instanceof HTMLInputElement || node instanceof HTMLTextAreaElement) && node.readOnly) ||
    node.getAttribute("aria-readonly") === "true";

  const candidates = document.querySelectorAll(
    'a[href],button,input,textarea,select,summary,[contenteditable="true"],' +
      roles.map((role) => `[role="${role}"]`).join(","),
  );

  const controls: Array<typeof Control.Type> = [];
  let truncated = candidates.length > 10_000;

  for (const node of Array.from(candidates).slice(0, 10_000)) {
    if (!(node instanceof HTMLElement)) continue;
    if (selector !== undefined && !node.matches(selector) && node.closest(selector) === null)
      continue;
    if (node instanceof HTMLInputElement && ["password", "file", "hidden"].includes(node.type))
      continue;
    if (
      !visible(node) ||
      !inView(node) ||
      node.matches(":disabled") ||
      node.closest('[aria-disabled="true"]')
    )
      continue;
    const kind = role(node);

    if (kind === null || (kind === "gridcell" && node.querySelector('button,[role="button"]')))
      continue;
    if (controls.length >= maximum) {
      truncated = true;
      break;
    }
    const ref = `${prefix}-${controls.length}`;
    const label = name(node) || kind;
    const currentValue = value(node);
    const readonly = readOnly(node);
    const allOptions = node instanceof HTMLSelectElement ? Array.from(node.options) : [];

    const matching = allOptions.flatMap((option, index) =>
      option.selected ||
      optionFilter === undefined ||
      `${option.label}\n${option.value}`.toLowerCase().includes(optionFilter.toLowerCase())
        ? [{ option, index }]
        : [],
    );

    const selected = matching.filter(({ option }) => option.selected);

    const options = [
      ...selected.slice(0, 256),
      ...matching
        .filter(({ option }) => !option.selected)
        .slice(0, Math.max(0, 256 - selected.length)),
    ];

    truncated ||= matching.length > options.length;
    registry.set(ref, node);
    checks.set(ref, {
      name: label,
      originalName: node.innerText.replace(/\s+/g, " ").trim().slice(0, 300),
      kind,
      viewport: true,
      verify: () =>
        role(node) === kind &&
        (name(node) || kind) === label &&
        value(node) === currentValue &&
        readOnly(node) === readonly &&
        node.closest('[aria-disabled="true"]') === null &&
        inView(node),
    });
    controls.push({
      ref,
      kind,
      name: label,
      value: currentValue,
      disabled: false,
      editable:
        !readonly &&
        (["textbox", "searchbox", "spinbutton"].includes(kind) ||
          (kind === "combobox" &&
            (node instanceof HTMLInputElement || node instanceof HTMLTextAreaElement))),
      pointerEvents: getComputedStyle(node).pointerEvents.slice(0, 64),
      options: options.map(({ option }) => option.value.slice(0, 4_096)),
      ...(node instanceof HTMLSelectElement
        ? {
            optionCount: allOptions.length,
            optionDetails: options.map(({ option, index }) => ({
              index,
              value: option.value.slice(0, 4_096),
              label: option.label.slice(0, 300),
              selected: option.selected,
              disabled:
                option.disabled ||
                (option.parentElement instanceof HTMLOptGroupElement &&
                  option.parentElement.disabled),
            })),
          }
        : {}),
      ...(node instanceof HTMLInputElement && ["checkbox", "radio"].includes(node.type)
        ? { checked: node.checked }
        : {}),
      attributes: Object.fromEntries(
        [
          "id",
          "name",
          "type",
          "role",
          "tabindex",
          "placeholder",
          "aria-label",
          "aria-labelledby",
          "aria-expanded",
          "aria-selected",
          "aria-checked",
          "aria-readonly",
          "aria-disabled",
          "aria-controls",
          "href",
        ].flatMap((key) => {
          const attribute = node.getAttribute(key);

          return attribute === null ? [] : [[key, attribute.slice(0, 1_024)]];
        }),
      ),
    });
  }

  const scopes =
    selector === undefined ? [document.body] : Array.from(document.querySelectorAll(selector));

  const visited = new Set<Node>();
  const words: Array<string> = [];
  const range = document.createRange();
  let length = 0;
  let scanned = 0;

  truncated ||= scopes.length > 256;
  for (const scope of scopes.slice(0, 256)) {
    if (scope === null) continue;
    const walker = document.createTreeWalker(scope, NodeFilter.SHOW_TEXT);
    let node = walker.nextNode();

    while (node !== null && length < 6_000 && scanned++ < 10_000) {
      const text = node.textContent?.trim() ?? "";
      const parent = node.parentElement;

      if (
        !visited.has(node) &&
        text &&
        parent &&
        !parent.closest("script,style,noscript,template") &&
        visible(parent)
      ) {
        range.selectNodeContents(node);
        const rect = range.getBoundingClientRect();

        if (
          rect.width > 0 &&
          rect.height > 0 &&
          rect.bottom > 0 &&
          rect.top < innerHeight &&
          rect.right > 0 &&
          rect.left < innerWidth
        ) {
          words.push(text);
          length += text.length;
        }
      }
      visited.add(node);
      node = walker.nextNode();
    }
  }
  const text = words.join("\n");

  return {
    text: text.slice(0, 6_000),
    controls,
    readyState: document.readyState,
    truncated: truncated || text.length > 6_000 || scanned >= 10_000,
  };
};
