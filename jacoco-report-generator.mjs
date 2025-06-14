import { createReadStream, existsSync, mkdirSync, writeFileSync } from 'fs';
import { dirname } from 'path';
import { createInterface } from 'readline';
import { request } from 'https';
import { execSync } from 'child_process';
import { fileURLToPath } from 'url';

// Get __dirname equivalent in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

/**
 * Simple CSV line parser that handles quoted fields
 * @param {string} line - CSV line to parse
 * @returns {string[]} Array of field values
 */
function parseCsvLine(line) {
    const result = [];
    let current = '';
    let inQuotes = false;
    let i = 0;
    
    while (i < line.length) {
        const char = line[i];
        
        if (char === '"') {
            // Check if this is an escaped quote
            if (i + 1 < line.length && line[i + 1] === '"') {
                current += '"';
                i += 2;
                continue;
            }
            inQuotes = !inQuotes;
        } else if (char === ',' && !inQuotes) {
            // End of field
            result.push(current);
            current = '';
        } else {
            current += char;
        }
        i++;
    }
    
    // Add the last field
    result.push(current);
    
    // Trim whitespace from each field
    return result.map(field => field.trim());
}

/**
 * Calculates coverage percentage
 * @param {number} covered - Number of covered items
 * @param {number} total - Total number of items
 * @returns {string} Formatted percentage string
 */
const calculateCoverage = (covered, total) => {
    const percentage = total > 0 ? (covered / total) * 100 : 100;
    return `${percentage.toFixed(2)}%`;
};

/**
 * Generates a markdown table from data
 * @param {Array} data - Array of objects containing row data
 * @param {Object} headers - Object mapping field names to display names
 * @param {number} [indent=0] - Number of tabs to indent the entire table
 * @returns {string} Markdown table
 */
const generateMarkdownTable = (data, headers, indent = 0) => {
    if (!data || data.length === 0) return '';

    const headerRow = Object.values(headers);
    const columns = Object.keys(headers);
    const tab = '  '; // 4 spaces per tab
    const indentStr = tab.repeat(indent);
    
    // Generate header separator
    const separator = headerRow.map(() => '---');
    
    // Generate rows
    const rows = data.map(item => 
        columns.map(field => item[field] || '')
    );
    
    // Combine all parts with indentation
    const table = [
        `${indentStr}| ${headerRow.join(' | ')} |`,
        `${indentStr}| ${separator.join(' | ')} |`,
        ...rows.map(row => `${indentStr}| ${row.join(' | ')} |`)
    ].join('\n');
    
    return table;
};

/**
 * Processes JaCoCo CSV and generates markdown report
 * @param {string} csvFilePath - Path to JaCoCo CSV file
 * @param {string} outputPath - Path to save the markdown report
 * @returns {Promise<string>} The generated markdown content
 */
const generateJacocoReport = (csvFilePath, outputPath) => {
    return new Promise((resolve, reject) => {
        const results = [];
        const packageCoverage = new Map();
        const classCoverage = new Map();

        if (!existsSync(csvFilePath)) {
            reject(new Error(`File not found: ${csvFilePath}`));
            return;
        }

        const fileStream = createReadStream(csvFilePath);
        
        fileStream.on('error', (err) => {
            reject(new Error(`Error reading file: ${err.message}`));
        });

        const rl = createInterface({
            input: fileStream,
            crlfDelay: Infinity
        });

        let headers = [];
        let isFirstLine = true;

        rl.on('line', (line) => {
            const values = parseCsvLine(line);
            
            if (isFirstLine) {
                headers = values;
                isFirstLine = false;
                return;
            }

            const row = {};
            headers.forEach((header, index) => {
                row[header] = values[index] || '';
            });

            results.push(row);
            
            // Extract package and class names
            const className = row['CLASS'] || '';
            const packageName = row['PACKAGE'];

            // Calculate coverage metrics
            const missed = parseInt(row['LINE_MISSED'] || '0', 10);
            const covered = parseInt(row['LINE_COVERED'] || '0', 10);
            const total = missed + covered;
            
            if (packageName) {
                const pkgData = packageCoverage.get(packageName) || { covered: 0, total: 0 };
                pkgData.covered += covered;
                pkgData.total += total;
                packageCoverage.set(packageName, pkgData);
            }
            
            if (className) {
                // Create fully qualified class name by combining package and class name
                const fullyQualifiedName = packageName ? `${packageName}.${className}` : className;
                classCoverage.set(fullyQualifiedName, {
                    covered,
                    total,
                    coverage: calculateCoverage(covered, total)
                });
            }
        });

        rl.on('close', () => {
            // Generate package coverage report
            const packageReport = Array.from(packageCoverage.entries())
                .map(([pkg, { covered, total }]) => ({
                    package: pkg,
                    coverage: calculateCoverage(covered, total),
                    'lines covered': covered,
                    'lines total': total
                }))
                .sort((a, b) => a.package.localeCompare(b.package));

            // Generate class coverage report
            const classReport = Array.from(classCoverage.entries())
                .map(([cls, { covered, total, coverage }]) => ({
                    class: cls,
                    coverage,
                    'lines covered': covered,
                    'lines total': total
                }))
                .sort((a, b) => a.class.localeCompare(b.class));

            // Generate markdown content
            const markdownContent = `## Coverage Report

${generateMarkdownTable(packageReport, {
                'package': 'Package',
                'coverage': 'Coverage',
                'lines covered': 'Lines Covered',
                'lines total': 'Total Lines'
            })}


<details>
  <summary>Class Coverage</summary>

${generateMarkdownTable(classReport, {
    'class': 'Class',
    'coverage': 'Coverage',
    'lines covered': 'Lines Covered',
    'lines total': 'Total Lines'}, 1)}

</details>
`;

            // Write to file
            mkdirSync(dirname(outputPath), { recursive: true });
            writeFileSync(outputPath, markdownContent);
            
            console.log(`Report generated successfully at: ${outputPath}`);
            resolve(markdownContent);
        });
    });
};

/**
 * Posts or updates a comment on a GitHub PR
 * @param {string} token - GitHub token with repo:write permission
 * @param {string} owner - Repository owner
 * @param {string} repo - Repository name
 * @param {number} prNumber - PR number
 * @param {string} comment - Comment content
 * @param {string} [commentId] - Optional comment ID to update
 */
async function postOrUpdateComment(token, owner, repo, prNumber, comment, commentId) {
    const data = JSON.stringify({
        body: comment
    });

    const options = {
        hostname: 'api.github.com',
        path: commentId 
            ? `/repos/${owner}/${repo}/issues/comments/${commentId}` // Update existing comment
            : `/repos/${owner}/${repo}/issues/${prNumber}/comments`, // Create new comment
        method: commentId ? 'PATCH' : 'POST',
        headers: {
            'Authorization': `token ${token}`,
            'User-Agent': 'Node.js',
            'Content-Type': 'application/json',
            'Content-Length': data.length
        }
    };

    return new Promise((resolve, reject) => {
        const req = request(options, (res) => {
            let response = '';
            
            res.on('data', (chunk) => {
                response += chunk;
            });

            res.on('end', () => {
                if (res.statusCode >= 200 && res.statusCode < 300) {
                    const result = JSON.parse(response || '{}');
                    resolve(result);
                } else {
                    reject(new Error(`GitHub API error: ${res.statusCode} - ${response}`));
                }
            });
        });

        req.on('error', (error) => {
            reject(error);
        });

        req.write(data);
        req.end();
    });
}

/**
 * Gets existing comments on a PR
 * @param {string} token - GitHub token
 * @param {string} owner - Repository owner
 * @param {string} repo - Repository name
 * @param {number} prNumber - PR number
 * @param {string} commentMarker - Unique marker to identify our comments
 * @returns {Promise<{id: string, body: string} | null>}
 */
async function findExistingComment(token, owner, repo, prNumber, commentMarker) {
    const options = {
        hostname: 'api.github.com',
        path: `/repos/${owner}/${repo}/issues/${prNumber}/comments`,
        method: 'GET',
        headers: {
            'Authorization': `token ${token}`,
            'User-Agent': 'Node.js',
            'Content-Type': 'application/json'
        }
    };

    return new Promise((resolve, reject) => {
        const req = request(options, (res) => {
            let response = '';
            
            res.on('data', (chunk) => {
                response += chunk;
            });

            res.on('end', () => {
                if (res.statusCode >= 200 && res.statusCode < 300) {
                    const comments = JSON.parse(response || '[]');
                    const existingComment = comments.find(comment => 
                        comment.body.includes(commentMarker)
                    );
                    resolve(existingComment || null);
                } else {
                    reject(new Error(`GitHub API error: ${res.statusCode} - ${response}`));
                }
            });
        });

        req.on('error', (error) => {
            reject(error);
        });

        req.end();
    });
}

// Get command line arguments
const args = process.argv.slice(2);

function printHelp() {
    console.log('JaCoCo Report Generator');
    console.log('Generates a markdown report from JaCoCo CSV data and can post to GitHub PRs\n');
    console.log('Basic usage:');
    console.log('  node jacoco-report.mjs <input-csv> <output-md> [--title "Report Title"]');
    console.log('  <input-csv>  Path to JaCoCo CSV file (e.g., target/site/jacoco/jacoco.csv)');
    console.log('  <output-md>  Path where to save the markdown report\n');
    console.log('GitHub PR comment usage:');
    console.log('  GITHUB_TOKEN=your_token node jacoco-report.mjs <input-csv> <output-md> \\');
    console.log('    --title "Report Title" \\');
    console.log('    --pr <pr-number> \\');
    console.log('    [--repo owner/repo]');
    console.log('  --title      Title to use for the report (default: "Coverage Report")');
    console.log('  --pr         PR number to post the comment to');
    console.log('  --repo       Repository in format owner/repo (defaults to current repo)');
    process.exit(1);
}

if (args.length < 2) {
    printHelp();
}

const inputFile = args[0];
const outputFile = args[1];

// Parse optional arguments
let title = 'Coverage Report';
let prNumber = null;
let repo = null;
let i = 2;

while (i < args.length) {
    const arg = args[i];
    if (arg === '--title' && i + 1 < args.length) {
        title = args[++i];
    } else if (arg === '--pr' && i + 1 < args.length) {
        prNumber = parseInt(args[++i], 10);
    } else if (arg === '--repo' && i + 1 < args.length) {
        repo = args[++i];
    } else if (arg === '--help' || arg === '-h') {
        printHelp();
    } else {
        console.error(`Unknown argument: ${arg}`);
        printHelp();
    }
    i++;
}

// Get GitHub token from environment
const githubToken = process.env.GITHUB_TOKEN;

// If PR number is provided but no token
if (prNumber && !githubToken) {
    console.error('Error: GITHUB_TOKEN environment variable is required when using --pr');
    process.exit(1);
}

// Get repository info if not provided
if (!repo) {
    try {
        const remoteUrl = execSync('git config --get remote.origin.url')
            .toString()
            .trim()
            .replace(/^git@github.com:/, 'https://github.com/')
            .replace(/\.git$/, '');
        
        const url = new URL(remoteUrl);
        repo = url.pathname.substring(1); // Remove leading slash
    } catch (error) {
        console.error('Error getting repository info:', error.message);
        process.exit(1);
    }
}

const [owner, repository] = repo.split('/');

// Main function to run the script
async function main() {
    try {
        const reportContent = await generateJacocoReport(inputFile, outputFile);
        
        if (prNumber && githubToken) {
            try {
                // Use the full file path in the comment marker and include the custom title
                const commentMarker = `<!-- jacoco-coverage-report:${inputFile} -->`;
                const comment = `${commentMarker}\n# ${title}\n\n${reportContent}`;
                
                // Find existing comment to update
                const existingComment = await findExistingComment(
                    githubToken,
                    owner,
                    repository,
                    prNumber,
                    commentMarker
                );
                
                // Post or update comment
                const result = await postOrUpdateComment(
                    githubToken,
                    owner,
                    repository,
                    prNumber,
                    comment,
                    existingComment?.id
                );
                
                console.log(`✅ Coverage report ${existingComment ? 'updated' : 'posted'} to PR #${prNumber}`);
            } catch (error) {
                console.error('Error posting to GitHub:', error.message);
                process.exit(1);
            }
        }
    } catch (error) {
        console.error('An error occurred:');
        console.error(error.message);
        process.exit(1);
    }
}

// Run the main function
main();
