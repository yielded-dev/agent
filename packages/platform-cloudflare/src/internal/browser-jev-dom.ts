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

import { pageFunction } from "./page-function.ts";

/** Isolated-realm Jev observation. The same private refs and native guards own input. */
export const inspectJevDom = pageFunction(
  (
    selector: string | undefined,
    prefix: string,
    maximum: number,
    optionFilter: string | undefined,
  ) => {
    const registry = new Map<string, Element>();

    const checks = new Map<
      string,
      {
        name: string;
        originalName: string;
        kind: string;
        viewport: boolean;
        inferred: boolean;
        verify: () => boolean;
      }
    >();

    Reflect.set(globalThis, "@effect-agent/native-browser", registry);
    Reflect.set(globalThis, "@effect-agent/native-browser-name-checks", checks);

    const visible = (node: Element) => {
      let ancestor: Element | null = node;

      while (ancestor !== null) {
        if (ancestor.matches('[aria-hidden="true"],[inert]')) return false;
        const root = ancestor.getRootNode();

        ancestor = ancestor.parentElement ?? (root instanceof ShadowRoot ? root.host : null);
      }

      return node.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true });
    };

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
      ((node instanceof HTMLInputElement || node instanceof HTMLTextAreaElement) &&
        node.readOnly) ||
      node.getAttribute("aria-readonly") === "true";

    const interactiveEvidence = (node: HTMLElement) => {
      if (node.matches("html,body")) return undefined;
      const root = node.getRootNode();
      const parent = node.parentElement ?? (root instanceof ShadowRoot ? root.host : null);

      const evidence =
        node.hasAttribute("tabindex") && node.tabIndex >= 0
          ? "focus"
          : getComputedStyle(node).cursor === "pointer" &&
              (parent === null || getComputedStyle(parent).cursor !== "pointer")
            ? "pointer"
            : undefined;

      if (evidence === undefined) return undefined;

      const closed: ReadonlyMap<Element, ShadowRoot> | undefined = Reflect.get(
        globalThis,
        "@effect-agent/native-browser-closed-roots",
      );

      const scopes: Array<Element | ShadowRoot> = [node];
      let scanned = 0;

      // A separate control owns its input, including across supported shadow roots.
      // Re-read the root map because dispatch refreshes newly attached closed roots.
      for (const scope of scopes) {
        const walker = document.createTreeWalker(scope, NodeFilter.SHOW_ELEMENT);
        let child: Node | null = scope instanceof Element ? scope : walker.nextNode();

        while (child instanceof Element) {
          if (++scanned > 10_000 || (child !== node && child.matches(controlSelector)))
            return undefined;
          const shadow = child.shadowRoot ?? closed?.get(child);

          if (shadow !== undefined && shadow !== null) scopes.push(shadow);
          child = walker.nextNode();
        }
      }

      return evidence;
    };

    const closedRoots: ReadonlyMap<Element, ShadowRoot> | undefined = Reflect.get(
      globalThis,
      "@effect-agent/native-browser-closed-roots",
    );

    const controlSelector =
      'a[href],button,input,textarea,select,summary,[contenteditable="true"],' +
      roles.map((role) => `[role="${role}"]`).join(",");

    const candidates: Array<Element> = [];
    const roots: Array<Document | ShadowRoot> = [document, ...(closedRoots?.values() ?? [])];
    const visitedRoots = new Set<Node>();
    const candidatesSeen = new Set<Node>();

    const filter: NodeFilter = {
      acceptNode(node) {
        return node instanceof Element &&
          node.matches('script,style,template,option,optgroup,[inert],[aria-hidden="true"]')
          ? NodeFilter.FILTER_REJECT
          : NodeFilter.FILTER_ACCEPT;
      },
    };

    let scanned = 0;
    let discovered = 0;
    let truncated = false;

    for (const root of roots) {
      if (visitedRoots.has(root)) continue;
      visitedRoots.add(root);
      const matches = selector === undefined ? [root] : Array.from(root.querySelectorAll(selector));

      truncated ||= matches.length > 256;
      for (const scope of matches.slice(0, 256)) {
        if (filter.acceptNode(scope) === NodeFilter.FILTER_REJECT) continue;
        const walker = document.createTreeWalker(scope, NodeFilter.SHOW_ELEMENT, filter);
        let node: Node | null = scope instanceof Element ? scope : walker.nextNode();

        while (node instanceof Element && scanned < 10_000) {
          if (!candidatesSeen.has(node)) {
            candidatesSeen.add(node);
            scanned++;
            candidates.push(node);
            const shadow = node.shadowRoot ?? closedRoots?.get(node);

            if (selector === undefined && shadow !== undefined && shadow !== null)
              roots.push(shadow);
          }
          node = walker.nextNode();
        }
        truncated ||= node !== null;
      }
      if (selector !== undefined) {
        // Discover shadow scopes separately; unrelated elements cannot consume the target's budget.
        const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT, filter);
        let node = walker.nextNode();

        while (node instanceof Element && discovered < 10_000) {
          discovered++;
          const shadow = node.shadowRoot ?? closedRoots?.get(node);

          if (shadow !== undefined && shadow !== null) roots.push(shadow);
          node = walker.nextNode();
        }
        truncated ||= node !== null;
      }
    }

    const controls: Array<typeof Control.Type> = [];
    // A modal dialog blocks background input; offer only its controls, as default observations do.
    const modal = document.querySelector("dialog:modal");

    for (const node of Array.from(candidates).slice(0, 10_000)) {
      if (!(node instanceof HTMLElement)) continue;
      if (node instanceof HTMLInputElement && ["password", "file", "hidden"].includes(node.type))
        continue;
      if (
        !visible(node) ||
        !inView(node) ||
        (modal !== null && !modal.contains(node)) ||
        node.matches(":disabled") ||
        node.closest('[aria-disabled="true"]')
      )
        continue;
      const semantic = node.matches(controlSelector);
      const evidence = semantic ? undefined : interactiveEvidence(node);

      const kind = semantic
        ? role(node)
        : evidence === undefined
          ? null
          : node.tagName.toLowerCase();

      if (kind === null || (kind === "gridcell" && node.querySelector('button,[role="button"]')))
        continue;

      const label =
        evidence === undefined
          ? name(node) || kind
          : name(node).replace(/\s+/g, " ").trim().slice(0, 300);

      if (label.length === 0) continue;
      if (controls.length >= maximum) {
        truncated = true;
        break;
      }
      const ref = `${prefix}-${controls.length}`;
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
        inferred: evidence !== undefined,
        verify: () =>
          (evidence === undefined
            ? role(node) === kind && (name(node) || kind) === label
            : !node.matches(controlSelector) &&
              interactiveEvidence(node) === evidence &&
              name(node).replace(/\s+/g, " ").trim().slice(0, 300) === label) &&
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
    let scannedText = 0;

    truncated ||= scopes.length > 256;
    for (const scope of scopes.slice(0, 256)) {
      if (scope === null) continue;
      const walker = document.createTreeWalker(scope, NodeFilter.SHOW_TEXT);
      let node = walker.nextNode();

      while (node !== null && length < 6_000 && scannedText++ < 10_000) {
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
      truncated: truncated || text.length > 6_000 || scannedText >= 10_000,
      page: {
        title: document.title,
        scrollY,
        viewportHeight: innerHeight,
        documentHeight: document.documentElement.scrollHeight,
      },
    };
  },
);
