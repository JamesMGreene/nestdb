## Pull requests
If you submit a pull request, thanks! There are a couple rules to follow though to make it manageable:
* The pull request should be atomic, i.e. contain only one feature. If it contains more, please submit multiple pull requests. Reviewing massive, 1000 loc+ pull requests is extremely hard.
* Likewise, if for one unique feature the pull request grows too large (more than 200 loc tests not included), please get in touch first.
* Please stick to the current coding style. It's important that the code uses a coherent style for readability.
* Do not include stylistic improvements ("housekeeping"). If you think one part deserves lots of housekeeping, use a separate pull request so as not to pollute the code.
* Don't forget tests for your new feature. Also don't forget to run the whole test suite before submitting to make sure you didn't introduce regressions.
* Do not build the browser version in your branch, I'll take care of it once the code is merged.
* Update the readme accordingly.
* Last but not least: keep in mind what NestDB's mindset is! The goal is not to be a replacement for MongoDB, but to have a pure JS database, easy to use, cross platform, fast and expressive enough for the target projects (small and self contained apps on server/desktop/browser/mobile). Sometimes it's better to shoot for simplicity than for API completeness with regards to MongoDB.

## Bug reporting guidelines
If you report a bug, thank you! That said for the process to be manageable please strictly adhere to the following guidelines. I'll not be able to handle bug reports that don't:
* Your bug report should be a self-containing gist complete with a package.json for any dependencies you need. I need to run through a simple `git clone gist; npm install; node bugreport.js`, nothing more.
* It should use assertions to showcase the expected vs actual behavior and be hysteresis-proof. It's quite simple in fact, see this example: https://gist.github.com/louischatriot/220cf6bd29c7de06a486
* Simplify as much as you can. Strip all your application-specific code. Most of the time you will see that there is no bug but an error in your code :)
* 50 lines max. If you need more, read the above point and rework your bug report. If you're **really** convinced you need more, please explain precisely in the issue.
* The code should be vanilla JavaScript; not CoffeeScript, not TypeScript, etc.
