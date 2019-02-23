#include <iostream>
#include "DBFile.h"
#include "test.h"
#include <gtest/gtest.h>

// make sure that the file path/dir information below is correct
const char *dbfile_dir = ""; // dir where binary heap files should be stored
const char *tpch_dir ="/Users/indrajit/Documents/Study/UFL/Spring19/DBI/Project/1/data/"; // dir where dbgen tpch files (extension *.tbl) can be found
const char *catalog_path = "catalog"; // full path of the catalog file

using namespace std;

TEST(OpenInvalidPath, Negative) {
	DBFile dbfile;
	int result = dbfile.Open ("invalidPath");
	ASSERT_TRUE(result == 0);
}

TEST(CreateEmptyFile, Positive) {
	DBFile dbfile;
	int result = dbfile.Create ("valid_file_path", heap, NULL);
	ASSERT_TRUE(result == 1);
	result = dbfile.Close();
	ASSERT_TRUE(result == 1);
}

TEST(CreateOpenSuccess, Positive) {
	DBFile dbfile;
	int result = dbfile.Create ("valid_file_path", heap, NULL);
	ASSERT_TRUE(result == 1);
	result = dbfile.Close();
	ASSERT_TRUE(result == 1);
	result = dbfile.Open ("valid_file_path");
	ASSERT_TRUE(result == 1);
}


// Assumption: Needs region.tbl to be present in 'tpch_dir'.
TEST(CreateFileLoadDataClose, Positive) {
	DBFile dbfile;
	int result = dbfile.Create ("file_with_data", heap, NULL);
	ASSERT_TRUE(result == 1);
	Schema schema(catalog_path, region);
	char tbl_path[100];
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, "region"); 
	dbfile.Load(schema, tbl_path);
	result = dbfile.Close();
	ASSERT_TRUE(result == 1);
}

// Assumption: Needs region.tbl to be present in 'tpch_dir'.
TEST(CreateLoadGetNext, Positive) {
	DBFile dbfile;
	int result = dbfile.Create ("file_with_data", heap, NULL);
	ASSERT_TRUE(result == 1);
	Schema schema(catalog_path, region);
	char tbl_path[100];
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, "region"); 
	dbfile.Load(schema, tbl_path);
	Record record;
	result = dbfile.GetNext (record);
	ASSERT_TRUE(result == 1);
	result = dbfile.Close();
	ASSERT_TRUE(result == 1);
}

// Assumption: Needs region.tbl to be present in 'tpch_dir'.
// Region file must contain just 5 records.
TEST(VerifyNumberOfRecords, Positive)
{
	DBFile dbfile;
	int result = dbfile.Create("file_with_data", heap, NULL);
	Schema schema(catalog_path, region);
	char tbl_path[100];
	sprintf(tbl_path, "%s%s.tbl", tpch_dir, "region");
	dbfile.Load(schema, tbl_path);
	int numberOfRecords = 0;
	Record temp;
	while (dbfile.GetNext(temp) == 1)
	{
		numberOfRecords++;
	}
	ASSERT_TRUE(numberOfRecords == 5);
	result = dbfile.Close();
	ASSERT_TRUE(result == 1);
}

TEST(CreateLoadGetNextAddGetNext, Positive) {
	DBFile dbfile;
	int result = dbfile.Create("file_with_data_v2", heap, NULL);
	Schema schema(catalog_path, region);
	char tbl_path[100];
	sprintf(tbl_path, "%s%s.tbl", tpch_dir, "region");
	dbfile.Load(schema, tbl_path);
	int numberOfRecords = 0;
	Record temp;
	while (dbfile.GetNext(temp) == 1)
	{
		numberOfRecords++;
		if (numberOfRecords == 2) break;
	}
	ASSERT_TRUE(numberOfRecords == 2);
	numberOfRecords = 0;
	dbfile.Load(schema, tbl_path);
	while (dbfile.GetNext(temp) == 1)
	{
		numberOfRecords++;
	}
	ASSERT_TRUE(numberOfRecords == 8);
	printf("printing numberOfRecords = %d << \n", numberOfRecords);
	result = dbfile.Close();
	ASSERT_TRUE(result == 1);
}

TEST(CreateEmptyFileWithoutClose, Positive) {
	DBFile dbfile;
	int result = dbfile.Create ("valid_file_path2", heap, NULL);
	ASSERT_TRUE(result == 1);
}

int main (int argc, char *argv[]) {
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}