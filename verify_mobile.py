import asyncio
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        # iPhone 12 viewport
        context = await browser.new_context(
            viewport={'width': 390, 'height': 844},
            user_agent='Mozilla/5.0 (iPhone; CPU iPhone OS 14_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1'
        )
        page = await context.new_page()
        await page.goto('http://localhost:8080')
        await asyncio.sleep(2)  # Wait for animations
        await page.screenshot(path='verification/screenshots/mobile_view.png')
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())
