import asyncio
from playwright.async_api import async_playwright
import os
import time

async def verify_jarvis():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        # Create a context with mock permissions for mic
        context = await browser.new_context(
            permissions=['microphone'],
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()

        print("Connecting to APEX JARVIS...")
        try:
            await page.goto("http://localhost:8080", wait_until="networkidle")
        except Exception as e:
            print(f"Failed to connect: {e}")
            await browser.close()
            return

        # 1. Initial Load Screenshot
        await page.screenshot(path="verification/screenshots/initial_load.png")
        print("Captured initial load.")

        # 2. Test Wallet Connect (Simulated)
        # Since we can't easily mock MetaMask in Playwright without extensions,
        # we'll check if the button exists and is clickable.
        connect_btn = page.locator("#connectWallet")
        await connect_btn.click()
        # We expect it to try and fail or show a message if window.ethereum is missing
        await page.wait_for_timeout(1000)
        await page.screenshot(path="verification/screenshots/wallet_interaction.png")
        print("Captured wallet interaction.")

        # 3. Test Command Input
        cmd_input = page.locator("#userInput")
        await cmd_input.fill("Research the future of AI and NFTs")
        await cmd_input.press("Enter")

        # Wait for task overlay to appear
        await page.wait_for_selector("#taskOverlay:not(.translate-y-full)", timeout=5000)
        await page.screenshot(path="verification/screenshots/command_executed.png")
        print("Captured command execution.")

        # 4. Wait for progress
        await page.wait_for_timeout(3000)
        await page.screenshot(path="verification/screenshots/progress_update.png")
        print("Captured progress update.")

        # 5. Spiral Mode Toggle
        spiral_btn = page.locator("#spiralToggle")
        await spiral_btn.click()
        await page.wait_for_timeout(1000)
        await page.screenshot(path="verification/screenshots/spiral_mode.png")
        print("Captured spiral mode.")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(verify_jarvis())
