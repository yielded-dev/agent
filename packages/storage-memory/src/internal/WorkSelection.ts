/** Keep only the lowest distinct metadata keys after a cursor while scanning resident indexes. */
export const boundedWorkSelection = <A>(
  limit: number,
  key: (value: A) => string,
  after?: string,
) => {
  const values: Array<A> = [];

  return {
    values,
    add: (value: A) => {
      const candidate = key(value);

      if (after !== undefined && candidate <= after) return;
      let low = 0;
      let high = values.length;

      while (low < high) {
        const middle = Math.floor((low + high) / 2);

        if (key(values[middle]) < candidate) low = middle + 1;
        else high = middle;
      }
      if (low >= limit || (low < values.length && key(values[low]) === candidate)) return;
      if (values.length === limit) values.pop();
      values.splice(low, 0, value);
    },
  };
};
