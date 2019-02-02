#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>

DBFile::DBFile()
{
    actualFile = new File();
    currentPage = new Page();
    currentReadPageIndex = 0;
    lastReturnedRecordIndex = 0;
    // By default we start off in read mode.
    inReadMode = true;
    // TODO: This is wrong, but okay for now.
    fileType = heap;
}

DBFile ::~DBFile()
{
    cout << "DBFile being destroyed\n";
    Close();
}

// Returns true if the file exists.
// false otherwise.
bool fileExists(const char *f_path)
{
    struct stat buf;
    return (stat(f_path, &buf) == 0);
}

void DBFile::initState(bool createFile, const char *f_path)
{
    // Create or open the file.
    actualFile->Open(createFile == true ? 0 : 1, f_path);
    // By default we start with read mode.
    // So fetch the first page if:
    //      - We are opening an existing file.
    //      - The file has data.
    if (!createFile && actualFile->GetLength() > 0) {
        actualFile->GetPage(currentPage, 0);
    }
}

int DBFile::Create(const char *f_path, fType f_type, void *startup)
{
    cout << "create called \n";
    // Assumption: if file already exists, it would be over written.
    initState(true, f_path);
    return 1;
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {
    cout << "Load called. \n";
    FILE *tableFile = fopen(loadpath, "r");
    ComparisonEngine comp;
    Record temp;
    while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {
       Add(temp);
    }
    cout << "Done with records !!!!! \n";
}

int DBFile::Open(const char *f_path)
{
    // Return failure if we can't find the file at
    // the given path.
    if (!fileExists(f_path))
        return 0;
    cout << "File open called \n";
    initState(false, f_path);
    return 1;
}

void DBFile::MoveFirst()
{
    // Move the read indexes back to zero.
    currentReadPageIndex = 0;
    lastReturnedRecordIndex = 0;
}

int DBFile::Close()
{
    cout << "Close called \n";
    if (actualFile != NULL)
    {
        actualFile->Close();
        delete actualFile;
        actualFile = NULL;
    }
    if (currentPage != NULL)
    {
        delete currentPage;
        currentPage = NULL;
    }
}

// Warning: Empties out the page passed-in.
void DBFile::updateToLastPage(Page *page)
{
    // Get rid of current data.
    page->EmptyItOut();
    // Get the last page.
    int length = actualFile->GetLength();
    if (length > 0)
    {
        actualFile->GetPage(page, length - 1);
    }
}

// This thing?
// Note that this function should actually consume addMe,
// so that after addMe has been put into the file, it cannot be used again.
void DBFile::Add(Record &rec)
{
    // If we are in read mode, switch to write mode and
    // fetch the last page.
    if (inReadMode)
    {
        inReadMode = false;
        // We won't be losing any data because of this page
        // switch, as dirty data gets written to disk when the mode
        // changed to read last time around.
        updateToLastPage(currentPage);
        Add(rec);
        return;
    }

    // We are in write mode, which means the page we have in memory now
    // is the last page to which we can write.
    if (currentPage->Append(&rec) == 0)
    {
        // Append failed-> Page is full.
        // Write this current page to disk.
        int length = actualFile->GetLength();
        actualFile->AddPage(currentPage, length);
        currentPage->EmptyItOut();
        Add(rec);
        return;
    }
}

int DBFile::GetNext(Record &fetchme)
{
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
}
