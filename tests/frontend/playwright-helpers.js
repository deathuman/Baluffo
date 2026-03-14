export async function stubPrompt(page, value = "Smoke User") {
  await page.addInitScript(name => {
    window.prompt = () => name;
  }, value);
}
