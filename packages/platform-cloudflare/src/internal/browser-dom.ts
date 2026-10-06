/// <reference lib="dom" />

import type { Control } from "@yielded/agent/browser-use";

import { pageFunction } from "./page-function.ts";

/** Executor-owned input settling, matching jev-ultrafast's frame/option readiness predicate. */
export const settleInputDom = pageFunction(
  (ref: string | undefined) =>
    new Promise<void>((resolve) => {
      const field: Element | undefined =
        ref === undefined
          ? undefined
          : Reflect.get(globalThis, "@effect-agent/native-browser")?.get(ref);

      const autocomplete = field?.getAttribute("role") === "combobox";
      let frames = 0;
      let stopped = false;
      let animation = 0;

      const finish = () => {
        if (stopped) return;
        stopped = true;
        clearTimeout(deadline);
        cancelAnimationFrame(animation);
        resolve();
      };

      const deadline = setTimeout(finish, autocomplete ? 200 : 50);

      const ready = () => {
        if (stopped) return;

        const ids = (field?.getAttribute("aria-controls") || field?.getAttribute("aria-owns") || "")
          .split(/\s+/)
          .filter(Boolean);

        const roots = ids.length ? ids.map((id) => document.getElementById(id)) : [document];

        const options = roots.flatMap((root) => [
          ...(root?.querySelectorAll('[role="option"]') ?? []),
        ]);

        if (
          ++frames >= 2 &&
          (!autocomplete ||
            options.some((element) => {
              const rect = element.getBoundingClientRect();

              return (
                rect.width &&
                rect.height &&
                rect.bottom > 0 &&
                rect.top < innerHeight &&
                element.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true })
              );
            }))
        )
          finish();
        else animation = requestAnimationFrame(ready);
      };

      animation = requestAnimationFrame(ready);
    }),
);

/** Read-only executor settling: 100 ms without a DOM mutation, bounded by one second. */
export const settleDom = pageFunction(
  () =>
    new Promise<boolean>((resolve) => {
      let quiet: ReturnType<typeof setTimeout>;

      const observer = new MutationObserver(() => {
        clearTimeout(quiet);
        quiet = setTimeout(() => finish(true), 100);
      });

      const finish = (settled: boolean) => {
        observer.disconnect();
        clearTimeout(quiet);
        clearTimeout(deadline);
        resolve(settled);
      };

      const deadline = setTimeout(() => finish(false), 1_000);

      observer.observe(document, {
        subtree: true,
        childList: true,
        attributes: true,
        characterData: true,
      });
      quiet = setTimeout(() => finish(true), 100);
    }),
);

/** Executed in the isolated page realm; the page cannot forge the reference registry. */
export const inspectDom = pageFunction(
  (
    selector: string | undefined,
    prefix: string,
    maximum: number,
    optionFilter: string | undefined,
    viewportOnly = false,
  ) => {
    const registry = new Map<string, Element>();

    const nameChecks = new Map<
      string,
      { name: string; originalName: string; verify: () => boolean }
    >();

    Reflect.set(globalThis, "@effect-agent/native-browser", registry);
    Reflect.set(globalThis, "@effect-agent/native-browser-name-checks", nameChecks);
    const roots: Array<Document | ShadowRoot> = [document];
    const visited = new Set<Node>();
    let discoveredElements = 0;
    const candidates: Array<HTMLElement> = [];
    let scanned = 0;
    let truncated = false;
    let text = "";

    const controlSelector =
      'button,a[href],input,textarea,select,label,[contenteditable="true"],[role="button"],[role="link"],[role="checkbox"],[role="radio"],[role="switch"],[role="tab"],[role="option"],[role="combobox"],[role="textbox"],[role="menuitem"],[role="scrollbar"]';

    const filter: NodeFilter = {
      acceptNode(node) {
        if (!(node instanceof Element)) return NodeFilter.FILTER_ACCEPT;
        const style = getComputedStyle(node);

        return node.matches('script,style,template,option,optgroup,[inert],[aria-hidden="true"]') ||
          style.display === "none" ||
          style.opacity === "0" ||
          style.contentVisibility === "hidden"
          ? NodeFilter.FILTER_REJECT
          : NodeFilter.FILTER_ACCEPT;
      },
    };

    for (let index = 0; index < roots.length && !truncated; index++) {
      const root = roots[index];

      if (root === undefined) break;

      // Narrow inspection starts at matching roots, so unrelated page content
      // cannot exhaust its budget. Shadow-host discovery has a separate bound.
      if (selector !== undefined) {
        const discovery = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT, filter);
        let node = discovery.nextNode();

        while (node !== null) {
          if (++discoveredElements > 10_000) {
            truncated = true;
            break;
          }
          if (node instanceof Element && node.shadowRoot !== null) roots.push(node.shadowRoot);
          node = discovery.nextNode();
        }
      }
      const matches = selector === undefined ? [root] : Array.from(root.querySelectorAll(selector));
      const scopes = matches.slice(0, 256);
      const nodes: Array<Element> = [];

      truncated ||= matches.length > scopes.length;
      for (const scope of scopes) {
        if (scope instanceof Element && filter.acceptNode(scope) === NodeFilter.FILTER_REJECT)
          continue;

        const walker = document.createTreeWalker(
          scope,
          NodeFilter.SHOW_ELEMENT | NodeFilter.SHOW_TEXT,
          filter,
        );

        let node: Node | null = scope instanceof Element ? scope : walker.nextNode();

        while (node !== null) {
          if (visited.has(node)) {
            node = walker.nextNode();
            continue;
          }
          visited.add(node);
          if (++scanned > 10_000) {
            truncated = true;
            break;
          }
          if (node instanceof Element) {
            nodes.push(node);
            if (selector === undefined && node.shadowRoot !== null) roots.push(node.shadowRoot);
          } else if (node instanceof Text) {
            const parent = node.parentElement;

            // Select options have their own bounded lookup; collapsed option text
            // must not bury the current form's values and validation messages.
            if (
              parent instanceof HTMLElement &&
              parent.closest('select,script,style,[inert],[aria-hidden="true"]') === null &&
              parent.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true })
            ) {
              const value = node.textContent?.replace(/\s+/g, " ").trim() ?? "";

              if (value.length > 0) {
                const room = Math.max(0, 12_000 - text.length);

                text += `${value.slice(0, room)}\n`;
                truncated ||= value.length >= room;
              }
            }
          }
          node = walker.nextNode();
        }
        if (scanned > 10_000) break;
      }
      for (const node of nodes) {
        if (selector !== undefined && !node.matches(selector) && node.closest(selector) === null)
          continue;
        if (!(node instanceof HTMLElement)) continue;

        const scrollable =
          node.scrollHeight > node.clientHeight &&
          ["auto", "scroll"].includes(getComputedStyle(node).overflowY);

        if (!node.matches(controlSelector) && !scrollable) continue;
        if (
          !node.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true }) ||
          node.closest('[inert],[aria-hidden="true"]') !== null
        )
          continue;
        const modal = document.querySelector("dialog:modal");

        if (modal !== null && !modal.contains(node) && node.getRootNode() === document) continue;
        candidates.push(node);
      }
    }

    let retained = candidates;

    // A popup falls back to off-screen controls only when none of its controls
    // are in view. Do not restore an entire off-screen calendar beside visible days.
    if (viewportOnly || candidates.length > maximum) {
      const visible: Array<HTMLElement> = [];
      const popupControls = new Map<Element, Array<HTMLElement>>();
      const visiblePopups = new Set<Element>();
      const offscreen: Array<HTMLElement> = [];

      const popupSelector =
        'dialog[open],[role="dialog"],[role="listbox"],[role="menu"],:popover-open';

      for (const node of candidates) {
        const rect = node.getBoundingClientRect();
        const x = rect.x + rect.width / 2;
        const y = rect.y + rect.height / 2;

        const inView = x >= 0 && x < innerWidth && y >= 0 && y < innerHeight;

        if (viewportOnly) {
          let popup = node.closest(popupSelector);

          if (popup?.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true })) {
            if (!inView) {
              const group = popupControls.get(popup) ?? [];

              group.push(node);
              popupControls.set(popup, group);
              continue;
            }
            while (popup !== null) {
              visiblePopups.add(popup);
              popup = popup.parentElement?.closest(popupSelector) ?? null;
            }
          }
        }
        (inView ? visible : offscreen).push(node);
      }

      const preferred = [
        ...visible,
        ...[...popupControls].flatMap(([popup, nodes]) => (visiblePopups.has(popup) ? [] : nodes)),
      ];

      retained = (
        viewportOnly ? (preferred.length > 0 ? preferred : offscreen) : [...visible, ...offscreen]
      ).slice(0, maximum);
      truncated ||= candidates.length > retained.length;
    }

    const readName = (node: HTMLElement) => {
      const associated = node instanceof HTMLLabelElement ? (node.control ?? node) : node;

      const nativeField =
        associated instanceof HTMLInputElement ||
        associated instanceof HTMLTextAreaElement ||
        associated instanceof HTMLSelectElement;

      const labels = nativeField ? associated.labels : null;
      const root = node.getRootNode();

      const labelledBy = (node.getAttribute("aria-labelledby") ?? "")
        .split(/\s+/)
        .map((id) =>
          root instanceof Document || root instanceof ShadowRoot
            ? (root.getElementById(id)?.textContent ?? "")
            : "",
        )
        .join(" ");

      return (
        node.getAttribute("aria-label") ||
        labelledBy.trim() ||
        (labels === null
          ? ""
          : Array.from(labels)
              .map((label) => label.textContent)
              .join(" ")) ||
        (nativeField
          ? node.getAttribute("placeholder")
          : node instanceof HTMLElement
            ? node.innerText
            : "") ||
        ""
      )
        .replace(/\s+/g, " ")
        .trim()
        .slice(0, 300);
    };

    const controls = retained.map((node, index) => {
      const ref = `${prefix}-${index}`;
      const tag = node.tagName.toLowerCase();
      const associated = node instanceof HTMLLabelElement ? (node.control ?? node) : node;
      const type = associated instanceof HTMLInputElement ? associated.type : "";

      const nativeField =
        associated instanceof HTMLInputElement ||
        associated instanceof HTMLTextAreaElement ||
        associated instanceof HTMLSelectElement;

      const role = node.getAttribute("role");

      const kind =
        tag === "label" &&
        associated instanceof HTMLInputElement &&
        ["checkbox", "radio"].includes(type)
          ? type
          : (role ?? (type === "checkbox" || type === "radio" ? type : tag === "a" ? "link" : tag));

      const name = readName(node);

      const allOptions = node instanceof HTMLSelectElement ? Array.from(node.options) : [];

      const matchingOptions = allOptions.flatMap((option, index) =>
        option.selected ||
        optionFilter === undefined ||
        `${option.label}\n${option.value}`.toLowerCase().includes(optionFilter.toLowerCase())
          ? [{ option, index }]
          : [],
      );

      // Keep selected state even when it falls beyond the bounded lookup window.
      const selectedOptions = matchingOptions.filter(({ option }) => option.selected);

      const visibleOptions = [
        ...selectedOptions.slice(0, 256),
        ...matchingOptions
          .filter(({ option }) => !option.selected)
          .slice(0, Math.max(0, 256 - selectedOptions.length)),
      ];

      if (matchingOptions.length > visibleOptions.length) truncated = true;

      const attributes = Object.fromEntries(
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
          "aria-controls",
          "href",
        ].flatMap((key) => {
          const value = node.getAttribute(key);

          return value === null ? [] : [[key, value.slice(0, 1_024)]];
        }),
      );

      registry.set(ref, node);
      nameChecks.set(ref, {
        name,
        originalName: name,
        verify: () => readName(node) === name,
      });
      const pointerEvents = getComputedStyle(node).pointerEvents?.slice(0, 64);

      return {
        ref,
        kind,
        name,
        attributes,
        ...(pointerEvents ? { pointerEvents } : {}),
        value:
          nativeField && type !== "password"
            ? associated.value.slice(0, 4_096)
            : node.isContentEditable
              ? node.innerText.slice(0, 4_096)
              : "",
        options: visibleOptions.map(({ option }) => option.value.slice(0, 4_096)),
        ...(node instanceof HTMLSelectElement
          ? {
              optionCount: allOptions.length,
              optionDetails: visibleOptions.map(({ option, index }) => ({
                index,
                value: option.value.slice(0, 4096),
                label: option.label.slice(0, 300),
                disabled:
                  option.disabled ||
                  (option.parentElement instanceof HTMLOptGroupElement &&
                    option.parentElement.disabled),
                selected: option.selected,
              })),
            }
          : {}),
        disabled:
          associated.matches(":disabled") || associated.getAttribute("aria-disabled") === "true",
        editable:
          (node instanceof HTMLInputElement &&
            !node.readOnly &&
            !["password", "file", "checkbox", "radio", "hidden", "submit", "button"].includes(
              node.type,
            )) ||
          (node instanceof HTMLTextAreaElement && !node.readOnly) ||
          (node.isContentEditable && node.getAttribute("aria-readonly") !== "true"),
        ...(associated instanceof HTMLInputElement && (type === "checkbox" || type === "radio")
          ? { checked: associated.checked }
          : {}),
      };
    });

    const names = new Map<string, number>();

    for (const control of controls) names.set(control.name, (names.get(control.name) ?? 0) + 1);

    const explicitLabel = (node: Element) => {
      const root = node.getRootNode();

      return (
        node.getAttribute("aria-label") ||
        (node.getAttribute("aria-labelledby") ?? "")
          .split(/\s+/)
          .map((id) =>
            root instanceof Document || root instanceof ShadowRoot
              ? (root.getElementById(id)?.textContent ?? "")
              : "",
          )
          .join(" ")
      )
        .replace(/\s+/g, " ")
        .trim();
    };

    for (const [index, control] of controls.entries()) {
      const node = retained[index];

      if (node === undefined || control.name.length === 0 || (names.get(control.name) ?? 0) < 2)
        continue;
      let branch: Element = node;

      for (let scope = node.parentElement; scope !== null; scope = scope.parentElement) {
        let source: Element | undefined = explicitLabel(scope) ? scope : undefined;
        let read = () => (source === undefined ? "" : explicitLabel(source));

        if (source === undefined) {
          for (
            let previous = branch.previousElementSibling;
            previous !== null;
            previous = previous.previousElementSibling
          ) {
            if (
              !(previous instanceof HTMLElement) ||
              !previous.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true }) ||
              previous.closest('[inert],[aria-hidden="true"]') !== null ||
              previous.matches(controlSelector) ||
              previous.querySelector(controlSelector) !== null ||
              (previous.childElementCount > 0 &&
                !previous.matches('h1,h2,h3,h4,h5,h6,caption,figcaption,legend,[role="heading"]'))
            )
              continue;
            const caption = previous.innerText.replace(/\s+/g, " ").trim();

            // Short static leaf text also serves as a caption in unlabelled containers.
            if (caption.length === 0 || caption.length > 120) continue;
            source = previous;
            const captionElement = previous;

            read = () => captionElement.innerText.replace(/\s+/g, " ").trim();
            break;
          }
        }
        const context = read();

        if (source !== undefined && context.length > 0) {
          const originalName = control.name;
          const container = scope;
          const caption = source;

          control.name = `${originalName} (${context})`.slice(0, 300);
          // Keep raw-name and contextual identity checks in the isolated realm.
          // Presentation must not weaken the existing native dispatch guard.
          nameChecks.set(control.ref, {
            name: control.name,
            originalName,
            verify: () =>
              readName(node) === originalName &&
              container.isConnected &&
              container.contains(node) &&
              container.contains(caption) &&
              (caption === container ||
                (caption.compareDocumentPosition(node) & Node.DOCUMENT_POSITION_FOLLOWING) !== 0) &&
              read() === context,
          });
          break;
        }
        branch = scope;
      }
    }

    return {
      text: text.slice(0, 12_000),
      controls,
      readyState: document.readyState,
      truncated: truncated || text.length > 12_000,
    };
  },
);

/** Current state after standard DOM scrolling. Pointer actions also require a hit test;
 * keyboard actions verify native focus at dispatch instead. Never dispatches input.
 */
export const checkDom = pageFunction(
  (target: Element | string, expected: typeof Control.Type, scroll: boolean, pointer: boolean) => {
    // A ref is looked up in this realm's registry, saving a handle round trip each way.
    const node: unknown =
      typeof target === "string"
        ? Reflect.get(globalThis, "@effect-agent/native-browser")?.get(target)
        : target;

    if (!(node instanceof HTMLElement) || !node.isConnected) return false;

    const nameCheck:
      | {
          name: string;
          originalName: string;
          kind?: string;
          viewport?: boolean;
          verify: () => boolean;
        }
      | undefined = Reflect.get(globalThis, "@effect-agent/native-browser-name-checks")?.get(
      expected.ref,
    );

    if (nameCheck !== undefined && (nameCheck.name !== expected.name || !nameCheck.verify()))
      return false;
    // Jev refs require their centers to remain in view. Do not move a visible
    // form or popup just to prepare input; the same native hit test still applies.
    if (scroll && !nameCheck?.viewport)
      node.scrollIntoView({ block: "center", inline: "center", behavior: "instant" });
    const associated = node instanceof HTMLLabelElement ? (node.control ?? node) : node;
    const type = associated instanceof HTMLInputElement ? associated.type : "";

    if (
      associated instanceof HTMLSelectElement &&
      expected.optionDetails !== undefined &&
      ((expected.optionCount === undefined
        ? expected.optionDetails.length
        : expected.optionCount) !== associated.options.length ||
        expected.optionDetails.some((option, index) => {
          const current = associated.options.item(option.index ?? index);

          return (
            current === null ||
            current.value !== option.value ||
            current.label.slice(0, 300) !== option.label ||
            current.selected !== option.selected ||
            (current.disabled ||
              (current.parentElement instanceof HTMLOptGroupElement &&
                current.parentElement.disabled)) !== option.disabled
          );
        }))
    )
      return false;
    if (node.isContentEditable && node.innerText.slice(0, 4_096) !== expected.value) return false;

    const kind =
      nameCheck?.kind ??
      (node instanceof HTMLLabelElement &&
      associated instanceof HTMLInputElement &&
      ["checkbox", "radio"].includes(type)
        ? type
        : (node.getAttribute("role") ??
          (type === "checkbox" || type === "radio"
            ? type
            : node.tagName === "A"
              ? "link"
              : node.tagName.toLowerCase())));

    if (
      kind !== expected.kind ||
      associated.matches(":disabled") ||
      associated.getAttribute("aria-disabled") === "true" ||
      node.closest('[inert],[aria-hidden="true"]') !== null ||
      !node.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true })
    )
      return false;
    if (
      (associated instanceof HTMLInputElement ||
        associated instanceof HTMLTextAreaElement ||
        associated instanceof HTMLSelectElement) &&
      type !== "password" &&
      associated.value.slice(0, 4_096) !== expected.value
    )
      return false;
    if (
      !(
        associated instanceof HTMLInputElement ||
        associated instanceof HTMLTextAreaElement ||
        associated instanceof HTMLSelectElement
      ) &&
      !node.isContentEditable &&
      !node.hasAttribute("aria-label") &&
      !node.hasAttribute("aria-labelledby") &&
      node.innerText.replace(/\s+/g, " ").trim().slice(0, 300) !==
        (nameCheck?.originalName ?? expected.name)
    )
      return false;
    if (
      expected.checked !== undefined &&
      (!(associated instanceof HTMLInputElement) || associated.checked !== expected.checked)
    )
      return false;
    if (
      Object.entries(expected.attributes ?? {}).some(
        ([key, value]) => (node.getAttribute(key) ?? "").slice(0, 1_024) !== value,
      )
    )
      return false;
    const ancestors = new Set<Node>();
    let ancestor: Node | null = node;

    while (ancestor !== null) {
      ancestors.add(ancestor);
      ancestor = ancestor instanceof ShadowRoot ? ancestor.host : ancestor.parentNode;
    }

    const modalSelector =
      'dialog:modal,[role="dialog"][aria-modal="true"],[role="alertdialog"][aria-modal="true"]';

    const roots = new Set([document, node.getRootNode()]);

    for (const root of roots) {
      if (!(root instanceof Document || root instanceof ShadowRoot)) continue;
      for (const modal of root.querySelectorAll(modalSelector)) {
        if (
          modal instanceof HTMLElement &&
          modal.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true }) &&
          !ancestors.has(modal)
        )
          return false;
      }
    }
    const rect = node.getBoundingClientRect();

    const x =
      Math.max(0, rect.left) + (Math.min(innerWidth, rect.right) - Math.max(0, rect.left)) / 2;

    const y =
      Math.max(0, rect.top) + (Math.min(innerHeight, rect.bottom) - Math.max(0, rect.top)) / 2;

    let hit = document.elementFromPoint(x, y);

    while (hit?.shadowRoot !== null && hit?.shadowRoot !== undefined) {
      const next = hit.shadowRoot.elementFromPoint(x, y);

      if (next === null || next === hit) break;
      hit = next;
    }

    // Keyboard-only overlays can sit above sibling card content. Require that
    // containing element to remain unobstructed, then verify native focus at dispatch.
    // This never permits a pointer click through the overlay.
    return rect.width > 0 &&
      rect.height > 0 &&
      node.isConnected &&
      (hit === node ||
        (hit !== null &&
          (node.contains(hit) ||
            (!pointer &&
              node.tabIndex >= 0 &&
              getComputedStyle(node).pointerEvents === "none" &&
              node.parentElement?.contains(hit)))))
      ? { x, y }
      : false;
  },
);

/** Translate a checked child point and refuse input through hidden/covered frame owners. */
export const checkFrameDom = pageFunction(
  (node: Element, point: { x: number; y: number }, scroll: boolean) => {
    if (
      !(node instanceof HTMLElement) ||
      !node.isConnected ||
      node.closest('[inert],[aria-hidden="true"]') !== null ||
      !node.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true })
    )
      return false;
    if (scroll) node.scrollIntoView({ block: "center", inline: "center", behavior: "instant" });
    const style = getComputedStyle(node);

    // A transformed coordinate space needs a native geometry adapter; never guess.
    let ancestor: Element | null = node;

    while (ancestor !== null) {
      const current = getComputedStyle(ancestor);

      if (current.transform !== "none" || (current.zoom !== "1" && current.zoom !== "normal"))
        return false;
      ancestor = ancestor.parentElement;
    }
    const rect = node.getBoundingClientRect();

    const x =
      rect.left + parseFloat(style.borderLeftWidth) + parseFloat(style.paddingLeft) + point.x;

    const y = rect.top + parseFloat(style.borderTopWidth) + parseFloat(style.paddingTop) + point.y;
    const hit = document.elementFromPoint(x, y);

    return hit === node ? { x, y } : false;
  },
);

export const waitDom = pageFunction((selector: string, state: string, text: string | undefined) => {
  const roots: Array<Document | ShadowRoot> = [document];
  const expectedText = text?.replace(/\s+/g, " ").trim();
  let scanned = 0;

  for (let index = 0; index < roots.length; index++) {
    const root = roots[index];

    if (root === undefined) break;

    const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT, {
      acceptNode(node) {
        if (!(node instanceof Element)) return NodeFilter.FILTER_ACCEPT;
        const style = getComputedStyle(node);

        return node.matches('script,style,template,option,optgroup,[inert],[aria-hidden="true"]') ||
          style.display === "none" ||
          style.opacity === "0" ||
          style.contentVisibility === "hidden"
          ? NodeFilter.FILTER_REJECT
          : NodeFilter.FILTER_ACCEPT;
      },
    });

    let node = walker.nextNode();

    while (node !== null) {
      if (++scanned > 10_000) return false;
      if (node instanceof Element) {
        if (node.shadowRoot !== null) roots.push(node.shadowRoot);
        if (
          node instanceof HTMLElement &&
          node.matches(selector) &&
          node.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true }) &&
          (expectedText === undefined ||
            node.innerText.replace(/\s+/g, " ").trim().includes(expectedText))
        ) {
          if (state === "hidden") return false;
          if (
            state === "visible" ||
            (state === "enabled" &&
              !node.matches(":disabled") &&
              node.getAttribute("aria-disabled") !== "true") ||
            (state === "text" && text !== undefined)
          )
            return true;
        }
      }
      node = walker.nextNode();
    }
  }

  return state === "hidden";
});
