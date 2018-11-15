#include "Joiner.hpp"
#include "Operators.hpp"
#include "Utils.hpp"
#include "gtest/gtest.h"
using namespace std;
namespace {
//---------------------------------------------------------------------------
class OperatorTest : public testing::Test {
  protected:
    Relation r1=Utils::createRelation(5,3);
    Relation r2=Utils::createRelation(10,5);
};
//---------------------------------------------------------------------------
TEST_F(OperatorTest, Scan) {
  unsigned relBinding=5;
  Scan scan(r1,relBinding);
  scan.require(SelectInfo(relBinding,0));
  scan.require(SelectInfo(relBinding,2));
  scan.run();
  auto results=scan.getResults();
  ASSERT_EQ(results.size(),2ull);
  auto colId1=scan.resolve(SelectInfo{relBinding,0});
  auto colId2=scan.resolve(SelectInfo{relBinding,2});
  ASSERT_EQ(results[colId1],r1.columns[0]);
  ASSERT_EQ(results[colId2],r1.columns[2]);
}
//---------------------------------------------------------------------------
TEST_F(OperatorTest, ScanWithSelection) {
  unsigned relId=0;
  unsigned relBinding=1;
  unsigned colId=2;
  SelectInfo sInfo(relId,relBinding,colId);
  uint64_t constant=2;
  {
    FilterInfo fInfo(sInfo,constant,FilterInfo::Comparison::Equal);
    FilterScan filterScan(r1,fInfo);
    filterScan.run();
    auto results=filterScan.getResults();
    ASSERT_EQ(results.size(),0u);
  }
  {
    FilterInfo fInfo(sInfo,constant,FilterInfo::Comparison::Equal);
    FilterScan filterScan(r1,fInfo);
    filterScan.require(SelectInfo(relBinding,0));
    filterScan.require(SelectInfo(relBinding,2));
    filterScan.run();

    ASSERT_EQ(filterScan.resultSize,1ull);
    auto results=filterScan.getResults();
    ASSERT_EQ(results.size(),2ull);
    auto filterColId=filterScan.resolve(SelectInfo{relBinding,colId});
    ASSERT_EQ(results[filterColId][0],constant);
  }
  {
    FilterInfo fInfo(sInfo,constant,FilterInfo::Comparison::Greater);
    FilterScan filterScan(r1,fInfo);
    colId=1;
    filterScan.require(SelectInfo(relBinding,colId));
    filterScan.run();

    ASSERT_EQ(filterScan.resultSize,2ull);
    auto results=filterScan.getResults();
    ASSERT_EQ(results.size(),1ull);

    auto resColId=filterScan.resolve(SelectInfo{relBinding,colId});
    auto filterCol=results[resColId];
    for (unsigned j=0;j<filterScan.resultSize;++j) {
      ASSERT_TRUE(filterCol[j]>constant);
    }
  }
}
//---------------------------------------------------------------------------
TEST_F(OperatorTest, Join) {
  unsigned lRid=0,rRid=1;
  unsigned r1Bind=0,r2Bind=1;
  unsigned lColId=1,rColId=3;

  Scan r1Scan(r1,r1Bind);
  Scan r2Scan(r2,r2Bind);


  {
    PredicateInfo pInfo(SelectInfo(lRid,r1Bind,lColId),SelectInfo(rRid,r2Bind,rColId));
    auto r1ScanPtr=make_unique<Scan>(r1Scan);
    auto r2ScanPtr=make_unique<Scan>(r2Scan);
    Join join(move(r1ScanPtr),move(r2ScanPtr),pInfo);
    join.run();
  }
  {
    // Self join
    PredicateInfo pInfo(SelectInfo(0,0,1),SelectInfo(0,1,2));
    Scan r1Scan2(r1,1);
    auto leftPtr=make_unique<Scan>(r1Scan);
    auto rightPtr=make_unique<Scan>(r1Scan2);
    Join join(move(leftPtr),move(rightPtr),pInfo);
    join.require(SelectInfo(r1Bind,0));
    join.run();

    ASSERT_EQ(join.resultSize,r1.size);

    auto resColId=join.resolve(SelectInfo{r1Bind,0});
    auto results=join.getResults();
    ASSERT_EQ(results.size(),1ull);
    auto resultCol=results[resColId];
    for (unsigned j=0;j<join.resultSize;++j) {
      ASSERT_EQ(resultCol[j],r1.columns[0][j]);
    }
  }
  {
    // Join r1 and r2 (should have same result as r1 and r1)
    auto leftPtr=make_unique<Scan>(r2Scan);
    auto rightPtr=make_unique<Scan>(r1Scan);
    PredicateInfo pInfo(SelectInfo(1,r2Bind,1),SelectInfo(0,r1Bind,2));
    Join join(move(leftPtr),move(rightPtr),pInfo);
    join.require(SelectInfo(r1Bind,1));
    join.require(SelectInfo(r2Bind,3));
    // Request a columns two times (should not have an effect)
    join.require(SelectInfo(r2Bind,3));
    join.run();

    ASSERT_EQ(join.resultSize,r1.size);

    auto resColId=join.resolve(SelectInfo{r2Bind,3});
    auto results=join.getResults();
    ASSERT_EQ(results.size(),2ull);
    auto resultCol=results[resColId];
    for (unsigned j=0;j<join.resultSize;++j) {
      ASSERT_EQ(resultCol[j],r1.columns[0][j]);
    }
  }
}
//---------------------------------------------------------------------------
TEST_F(OperatorTest, Checksum) {
  unsigned relBinding=5;
  Scan r1Scan(r1,relBinding);

  {
    auto r1ScanPtr=make_unique<Scan>(r1Scan);
    vector<SelectInfo> checkSumColumns;
    Checksum checkSum(move(r1ScanPtr),checkSumColumns);
    checkSum.run();
    ASSERT_EQ(checkSum.checkSums.size(),0ull);
  }
  {
    auto r1ScanPtr=make_unique<Scan>(r1Scan);
    vector<SelectInfo> checkSumColumns;
    checkSumColumns.emplace_back(0,relBinding,0);
    checkSumColumns.emplace_back(0,relBinding,2);
    Checksum checkSum(move(r1ScanPtr),checkSumColumns);
    checkSum.run();

    ASSERT_EQ(checkSum.checkSums.size(),2ull);
    uint64_t expectedSum=0;
    for (unsigned i=0;i<r1.size;++i) {
      expectedSum+=r1.columns[0][i];
    }
    ASSERT_EQ(checkSum.checkSums[0],expectedSum);
    ASSERT_EQ(checkSum.checkSums[1],expectedSum);
  }
  {
    SelectInfo sInfo(0,relBinding,2);
    uint64_t constant=3;
    FilterInfo fInfo(sInfo,constant,FilterInfo::Comparison::Equal);
    FilterScan r1ScanFilter(r1,fInfo);
    auto filterScanPtr=make_unique<FilterScan>(r1ScanFilter);
    vector<SelectInfo> checkSumColumns;
    checkSumColumns.emplace_back(0,relBinding,2);
    Checksum checkSum(move(filterScanPtr),checkSumColumns);
    checkSum.run();
    ASSERT_EQ(checkSum.checkSums.size(),1ull);
    ASSERT_EQ(checkSum.checkSums[0],constant);
  }
}
//---------------------------------------------------------------------------
TEST_F(OperatorTest, SelfJoin) {
  unsigned relBinding=5;
  Scan r1Scan(r1,relBinding);
  {
    PredicateInfo pInfo(SelectInfo(1,relBinding,1),SelectInfo(1,relBinding,2));
    SelfJoin selfJoin(make_unique<Scan>(r1Scan),pInfo);
    selfJoin.run();
    ASSERT_EQ(selfJoin.resultSize,r1.size);
    ASSERT_EQ(selfJoin.getResults().size(),0ull);
  }
  {
    PredicateInfo pInfo(SelectInfo(1,relBinding,1),SelectInfo(1,relBinding,2));
    SelfJoin selfJoin(make_unique<Scan>(r1Scan),pInfo);
    selfJoin.require(SelectInfo(relBinding,0));
    selfJoin.run();
    selfJoin.resolve(SelectInfo(relBinding,0));
    ASSERT_EQ(selfJoin.resultSize,r1.size);
    auto results=selfJoin.getResults();
    ASSERT_EQ(results.size(),1ull);
  }
}
//---------------------------------------------------------------------------
TEST_F(OperatorTest, Joiner) {
  Joiner joiner;
  unsigned numTuples=10;
  for (unsigned i=0;i<5;i++)
    joiner.relations.push_back(Utils::createRelation(numTuples,3));

  uint64_t sum=0;
  for (unsigned i=0;i<numTuples;++i) {
    sum+=joiner.relations[0].columns[0][i];
  }
  string expSumWithoutFilters=to_string(sum);
  {
    // Binary join without selections
    auto query="1 2|0.0=1.1|1.2";
    QueryInfo i(query);
    auto result=joiner.join(i);
    ASSERT_EQ(result,expSumWithoutFilters+"\n");
  }
  {
    // Query without selections
    auto query="0 2 3|0.0=1.1&1.2=2.0|2.2";
    QueryInfo i(query);
    auto result=joiner.join(i);
    ASSERT_EQ(result,expSumWithoutFilters+"\n");
  }
  {
    // Query with selection (=4)
    auto query="0 1 4|0.0=1.1&1.2=2.0&1.1=4|1.0 2.2";
    QueryInfo i(query);
    auto result=joiner.join(i);
    ASSERT_EQ(result,"4 4\n");
  }
  {
    // Query without result
    auto query="0 1 2|0.0=1.1&1.2=2.0&1.1=100|1.0 2.2";
    QueryInfo i(query);
    auto result=joiner.join(i);
    ASSERT_EQ(result,"NULL NULL\n");
  }
  {
    // Self join
    auto query="0 0|0.0=1.1|1.0";
    QueryInfo i(query);
    auto result=joiner.join(i);
    ASSERT_EQ(result,expSumWithoutFilters+"\n");
  }
  {
    // Cyclic query graph
    auto query="0 1 2|0.0=1.1&1.1=2.0&2.2=0.1|1.0";
    QueryInfo i(query);
    auto result=joiner.join(i);
    ASSERT_EQ(result,expSumWithoutFilters+"\n");
  }
  {
    // 4 Relations
    auto query="0 1 2 3|0.0=1.1&1.1=2.0&2.2=3.1|1.0";
    QueryInfo i(query);
    auto result=joiner.join(i);
    ASSERT_EQ(result,expSumWithoutFilters+"\n");
  }
  {
    // 4 Relations (Permuted)
    auto query="0 1 2 3|0.0=1.1&2.1=3.0&0.2=2.1|1.0";
    QueryInfo i(query);
    auto result=joiner.join(i);
    ASSERT_EQ(result,expSumWithoutFilters+"\n");
  }
  {
    // 2 Filters (Equal)
    auto query="0 1|0.0=1.1&0.0=3&1.0=3|1.0";
    QueryInfo i(query);
    auto result=joiner.join(i);
    ASSERT_EQ(result,"3\n");
  }
  {
    // 2 Filters (Distinct)
    auto query="0 1|0.0=1.1&0.0<3&1.0>3|1.0";
    QueryInfo i(query);
    auto result=joiner.join(i);
    ASSERT_EQ(result,"NULL\n");
  }
  {
    // Multiple Filters per relation
    auto query="0 1|0.0=1.1&0.0>1&0.0<3|1.0";
    QueryInfo i(query);
    auto result=joiner.join(i);
    ASSERT_EQ(result,"2\n");
  }
}
//---------------------------------------------------------------------------
}
