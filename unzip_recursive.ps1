# Input path to the ZIP file
$zipPath = "E:\Monthly Prescription Drug Plan Formulary and Pharmacy Network Information.zip"

# Function to recursively extract ZIP files
function Expand-ZipRecursive {
    param (
        [string]$ZipFile,
        [string]$DestinationPath
    )

    Write-Host "Extracting: $ZipFile to $DestinationPath"
    
    try {
        # Extract the current ZIP file
        Expand-Archive -Path $ZipFile -DestinationPath $DestinationPath -Force

        # Look for ZIP files in the extracted content
        Get-ChildItem -Path $DestinationPath -Recurse -Filter "*.zip" | ForEach-Object {
            $newDestination = Join-Path -Path $_.DirectoryName -ChildPath $_.BaseName
            
            # Create directory if it doesn't exist
            if (!(Test-Path -Path $newDestination)) {
                New-Item -ItemType Directory -Path $newDestination | Out-Null
            }
            
            # Recursively extract nested ZIP file
            Expand-ZipRecursive -ZipFile $_.FullName -DestinationPath $newDestination
        }
    }
    catch {
        Write-Error "Error extracting $ZipFile : $_"
    }
}

# Validate input ZIP file exists
if (!(Test-Path $zipPath)) {
    Write-Error "ZIP file not found: $zipPath"
    exit 1
}

# Create base destination path in the same drive as input file
$baseDestination = "E:\Extracted_Files"

# Create destination directory if it doesn't exist
if (!(Test-Path -Path $baseDestination)) {
    New-Item -ItemType Directory -Path $baseDestination | Out-Null
}

# Start the recursive extraction
Write-Host "Starting recursive ZIP extraction..."
Expand-ZipRecursive -ZipFile $zipPath -DestinationPath $baseDestination

Write-Host "Extraction complete! Files are in: $baseDestination"
