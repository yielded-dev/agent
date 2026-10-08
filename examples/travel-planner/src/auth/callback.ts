/** Inject into the rendered head at the Worker boundary: React hoists resources
 * ahead of ordinary script elements, even when they appear first in JSX. */
export const captureCallbackScript = `if(['/travel/auth/github/callback','/travel/auth/yielded/callback'].includes(location.pathname)){const q=location.search;history.replaceState(null,'',location.pathname);window.__elsewhereCallback=q;}`;
