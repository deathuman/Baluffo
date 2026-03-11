export function getVisiblePages(totalPages, currentPage) {
  if (totalPages <= 9) {
    return Array.from({ length: totalPages }, (_, idx) => idx + 1);
  }

  const pages = [1];
  let left = currentPage - 2;
  let right = currentPage + 2;

  if (left <= 2) {
    left = 2;
    right = 5;
  }

  if (right >= totalPages - 1) {
    right = totalPages - 1;
    left = totalPages - 4;
  }

  if (left > 2) pages.push("...");
  for (let p = left; p <= right; p++) pages.push(p);
  if (right < totalPages - 1) pages.push("...");
  pages.push(totalPages);

  return pages;
}
