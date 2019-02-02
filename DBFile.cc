#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>

DBFile::DBFile () {
    actualFile = new File();
    currentPage = new Page();
    currentReadPageIndex = 0;
    lastReturnedRecordIndex = 0;
    // TODO: This is wrong, but okay for now.
    fileType = heap;
}

DBFile :: ~DBFile () {
    cout << "DBFile being destroyed\n";
    Close();
}

// Returns true if the file exists.
// false otherwise.
// TODO: Remove if unused.
bool fileExists(const char* f_path) {
    struct stat buf;
    return (stat(f_path, &buf) == 0);
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    cout << "create called \n";
    // Assumption: if file already exists, it would be over written.
    actualFile->Open(0, strdup(f_path));
    return 1;
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
}

int DBFile::Open (const char *f_path) {
}

void DBFile::MoveFirst () {
}

int DBFile::Close () {
    cout << "Close called \n";
    if (actualFile != NULL) {
        actualFile->Close();
	    delete actualFile;
        actualFile = NULL;
    }
    if (currentPage != NULL) {
        delete currentPage;
        currentPage = NULL;
    }
}

void DBFile::Add (Record &rec) {
}

int DBFile::GetNext (Record &fetchme) {
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}

