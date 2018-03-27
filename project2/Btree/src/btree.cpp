/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <malloc.h>
#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"

#define DEBUG 0


std::string indexName;
using namespace std;

namespace badgerdb
{

	typedef struct tuple {
		int i;
		double d;
		char s[64];
	} RECORD;

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

	BTreeIndex::BTreeIndex(const std::string & relationName,
						   std::string & outIndexName,
						   BufMgr *bufMgrIn,
						   const int attrByteOffset,
						   const Datatype attrType)
	{

		bufMgr = bufMgrIn; //MEMBER VARIABLE

		// Step 1 : Make the file name and put it to outIndexName

		std::ostringstream idxStr ;
		idxStr << relationName << '.' << attrByteOffset ;
		indexName = idxStr.str() ;
		outIndexName = indexName;

		// Step 2 : Create the blob file if needed -> this is only done once, check if should be done incremental too

		if(!(file->exists(outIndexName))){

			file = new BlobFile(outIndexName, true);

			Page* bPage;

			bufMgr->allocPage(file, headerPageNum, bPage); //MEMBER VARIABLE -----------> start

			rootPageNum = 5000; //MEMBER VARIABLE
			attributeType = attrType; //MEMBER VARIABLE
			BTreeIndex::attrByteOffset = attrByteOffset; // MEMBER VARIABLE

			leafOccupancy = 0;
			nodeOccupancy = 0;
			scanExecuting = false;
			nextEntry = -1;
			currentPageNum = 5000;
			currentPageData = NULL;

			for(int c=0; c<10; c++){
				accessPath[c] = -1;
			}

			//IndexMetaInfo indexMetaInfo;
			strncpy(((IndexMetaInfo*)bPage)->relationName, relationName.c_str(), 10); // Sorry but will have to do this
			((IndexMetaInfo*)bPage)->attrByteOffset = attrByteOffset;
			((IndexMetaInfo*)bPage)->attrType = attrType;
			((IndexMetaInfo*)bPage)->rootPageNo = -1;

			bufMgr->unPinPage(file, headerPageNum, true); // ----------------------> end

			// I dont see closing option but I should somehow lets see
			{
				FileScan fscan(relationName, bufMgrIn);

				try
				{
					RecordId scanRid;
					while(1)
					{
						fscan.scanNext(scanRid);
						//Assuming RECORD.attrByteOffset is our key, lets extract the key, which we know is INTEGER and whose byte offset is also know inside the record.
						std::string recordStr = fscan.getRecord();
						const char *record = recordStr.c_str();

						if(attrType == 0){
							int key = *((int *)(record + offsetof (RECORD, i)));
							insertEntry((void *)&key, scanRid);
							//std::cout<< key <<std::endl;
							//bufMgr->printSelf();
							//return;
						}else if(attrType == 1){
							double key = *((double *)(record + offsetof (RECORD, d)));
							insertEntry((void *)&key, scanRid);
						}else{
							char* temp = (char *)(record + offsetof (RECORD, s));
							char key[STRINGSIZE];
							for(int c=0;c<STRINGSIZE;c++){
								key[c] = temp[c];
							}
							insertEntry((void *)key, scanRid);
						}
					}
				}catch(EndOfFileException e){

				}
			}
		}
		/*if(attrType == 0){
            lookUpInt();
        }*/
	}

	const void BTreeIndex::lookUpInt(){

		PageId currentPageNumber = rootPageNum;
		Page* currPage;
		int localNLCount = 0;
		NonLeafNodeInt *nlNode;
		while(localNLCount != 1){

			bufMgr->readPage(file, currentPageNumber, currPage);
			nlNode = (NonLeafNodeInt *)currPage;
			for(int i=0;i<INTARRAYNONLEAFSIZE;i++){
				if(nlNode->pageNoArray[i] != 5000){
					currentPageNumber = nlNode->pageNoArray[i];
					break;
				}
			}
			localNLCount = nlNode->level;
		}

		for(int i=0;i<INTARRAYNONLEAFSIZE;i++){
			std::cout<<nlNode->keyArray[i]<<" ";
		}
		std::cout<<"\n";

		while(localNLCount != 5000){
			bufMgr->readPage(file, currentPageNumber, currPage);
			LeafNodeInt *lNode = (LeafNodeInt *)currPage;
			for(int i=0;i<INTARRAYLEAFSIZE;i++){
				std::cout<<lNode->keyArray[i]<<" ";
			}
			std::cout<<"\n";
			std::cout<<"Going again\n";
			currentPageNumber = lNode->rightSibPageNo;
			localNLCount = lNode->rightSibPageNo;
		}

	}

	/*const void BTreeIndex::printNode(struct LeafNodeString* lNode){
		for(int i=0;i<STRINGARRAYLEAFSIZE&&lNode->keyArray[i][0]!='\0';i++){
			for(int c=0;c<10;c++){
				std::cout<<lNode->keyArray[i][c];;
			}
		}
		std::cout<<"done"<<std::endl;
	}*/

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

	BTreeIndex::~BTreeIndex()
	{
		leafOccupancy = 0;
		nodeOccupancy = 0;
		scanExecuting = false;
		nextEntry = -1;
		currentPageNum = 5000;
		currentPageData = NULL;

		for(int c=0; c<10; c++){
			accessPath[c] = -1;
		}

		bufMgr->flushFile(file);

		delete file;
		File::remove(indexName);
	}

// ----------------------------------------------------------------------------------------------
// BTreeIndex::initializeIntNode -- this initializes all the int keys to either -1 for leaf node
// ----------------------------------------------------------------------------------------------
	const void BTreeIndex::initializeLeaf(struct LeafNodeInt* lNode){
		for(int i=0;i<INTARRAYLEAFSIZE;i++){
			lNode->keyArray[i] = -1;
		}
	}

// ----------------------------------------------------------------------------------------------
// BTreeIndex::initializeIntNode -- this initializes all the int keys to either -1 for non-leaf node
// ----------------------------------------------------------------------------------------------
	const void BTreeIndex::initializeNonLeaf(struct NonLeafNodeInt* nlNode){
		int i;
		for(i=0;i<INTARRAYNONLEAFSIZE;i++){
			nlNode->keyArray[i] = -1;
			nlNode->pageNoArray[i] = 5000;
		}
		nlNode->pageNoArray[i+1] = 5000;
	}







// ----------------------------------------------------------------------------------------------
// BTreeIndex::addToParent -- adds entry to the parent
// ----------------------------------------------------------------------------------------------
	const void BTreeIndex::addToParent(const void *key, const PageId pid,const PageId pidr, const int localAccessLevel){

		int i;

		Page* tempPage;
		bufMgr->readPage(file, accessPath[localAccessLevel], tempPage);// ------------------> start
		NonLeafNodeInt* nlNode = (NonLeafNodeInt *)malloc(sizeof(NonLeafNodeInt));
		nlNode = (NonLeafNodeInt*)tempPage;


		if(localAccessLevel == -1){
			// Make the new root node
			PageId newRootPageNumber;
			Page *newRootPage;
			bufMgr->allocPage(file, newRootPageNumber, newRootPage); //-------- start

			initializeNonLeaf((NonLeafNodeInt *)newRootPage);

			((NonLeafNodeInt *)newRootPage)->pageNoArray[0] = pidr;
			((NonLeafNodeInt *)newRootPage)->keyArray[0] = *(int *)key;
			((NonLeafNodeInt *)newRootPage)->pageNoArray[1] = pid;

			Page* hPage;
			bufMgr->readPage(file, headerPageNum, hPage);// ------------------> start
			((IndexMetaInfo *)hPage)->rootPageNo = newRootPageNumber;
			bufMgr->unPinPage(file, headerPageNum, true);//----------------------end

			rootPageNum = newRootPageNumber;

			bufMgr->unPinPage(file, newRootPageNumber, true);  // ------------- end

			nodeOccupancy++;

			return;
		}

		for(i=0;i<INTARRAYNONLEAFSIZE && nlNode->keyArray[i] != -1;i++);

		if(i == INTARRAYNONLEAFSIZE){
			// I will make an array first
			int keyTemp[INTARRAYNONLEAFSIZE+1];
			PageId pidTemp[INTARRAYNONLEAFSIZE+2];
			int set = 0;

			// Force the 0th pageid here : not needed I guess this should work
			int j;
			for(i=0,j=0; i<INTARRAYNONLEAFSIZE+1; i++){
				if(!set && *(int *)key < nlNode->keyArray[j]){
					keyTemp[i] = *(int *)key;
					pidTemp[i+1] = pid;
					set = 1;
				}else{
					keyTemp[i] = nlNode->keyArray[j];
					pidTemp[i+1] = nlNode->pageNoArray[j];
					j++;
				}
			}

			// Copy it to first node
			for(i=0;i<INTARRAYLEAFSIZE/2;i++){
				nlNode->keyArray[i] = keyTemp[i];
				nlNode->pageNoArray[i+1] = pidTemp[i+1];
			}
			for(int n=i;n<INTARRAYLEAFSIZE;n++){
				nlNode->keyArray[n] = -1;
			}

			// Now make a second node
			PageId secondPageNumber;
			Page* tempPage;
			bufMgr->allocPage(file, secondPageNumber, tempPage); //--------------------start

			initializeNonLeaf((NonLeafNodeInt *)tempPage);

			((NonLeafNodeInt *)tempPage)->pageNoArray[0] = pidTemp[(INTARRAYNONLEAFSIZE/2)+2];

			for(int n=(INTARRAYNONLEAFSIZE/2)+1,j=0;n<INTARRAYNONLEAFSIZE+1;n++,j++){
				((NonLeafNodeInt *)tempPage)->keyArray[j] = keyTemp[n];
				((NonLeafNodeInt *)tempPage)->pageNoArray[j+1] = pidTemp[n+1];
			}

			bufMgr->unPinPage(file, secondPageNumber, true); //------------------------end

			bufMgr->unPinPage(file, accessPath[localAccessLevel], true);//----------------------end

			nodeOccupancy++;

			// Now send this entry to parent <--- check the localAccessLevel first
			if(localAccessLevel == 0){
				addToParent((void *)&keyTemp[(INTARRAYNONLEAFSIZE/2)+1], secondPageNumber, accessPath[localAccessLevel], localAccessLevel-1);
			}else{
				addToParent((void *)&keyTemp[(INTARRAYNONLEAFSIZE/2)+1], secondPageNumber, 0, localAccessLevel-1);
			}
		}else{


			for(i=0; nlNode->keyArray[i] != -1 && *(int *)key > nlNode->keyArray[i]; i++);

			if(nlNode->keyArray[i] == -1){
				nlNode->keyArray[i] = *(int *)key;
				nlNode->pageNoArray[i+1] = pid;
			}else{
				int n;
				for(n=i; nlNode->keyArray[n]!=-1; n++);
				while(n!=i){
					nlNode->keyArray[n] = nlNode->keyArray[n-1];
					nlNode->pageNoArray[n+1] = nlNode->pageNoArray[n];
					n--;
				}
				nlNode->keyArray[n] = *(int *)key;
				nlNode->pageNoArray[n+1] = pid;
			}

			bufMgr->unPinPage(file, accessPath[localAccessLevel], true);//----------------------end

		}

	}






// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

	const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
	{
		// Checking if root is already present
		if(rootPageNum == 5000){
			Page *leafBTreePage;
			Page *rootBTreePage;
			PageId leafBTPageNumber, rootBTPageNumber;

			bufMgr->allocPage(file, rootBTPageNumber, rootBTreePage);//---------------start
			bufMgr->allocPage(file, leafBTPageNumber, leafBTreePage);//---------------start

			Page* hPage;
			bufMgr->readPage(file, headerPageNum, hPage);// ------------------> start
			((IndexMetaInfo *)hPage)->rootPageNo = rootBTPageNumber;
			bufMgr->unPinPage(file, headerPageNum, true);//----------------------end

			rootPageNum = rootBTPageNumber;

			initializeLeaf((LeafNodeInt *)leafBTreePage);
			initializeNonLeaf((NonLeafNodeInt *)rootBTreePage);

			((NonLeafNodeInt *)rootBTreePage)->level = 1;
			((NonLeafNodeInt *)rootBTreePage)->keyArray[0] = *(int *)key;
			((NonLeafNodeInt *)rootBTreePage)->pageNoArray[1] = leafBTPageNumber;

			((LeafNodeInt *)leafBTreePage)->keyArray[0] = *(int *)key;
			((LeafNodeInt *)leafBTreePage)->ridArray[0] = rid;
			((LeafNodeInt *)leafBTreePage)->rightSibPageNo = 5000;

			//printNode((LeafNodeInt *)leafBTreePage);

			bufMgr->unPinPage(file, rootBTPageNumber, true);//---------------------------end
			bufMgr->unPinPage(file, leafBTPageNumber, true);//---------------------------end

			nodeOccupancy++;
			leafOccupancy++;

			// else we already have a root
		}else{

			// let us take care of non leaf nodes first
			Page *currentPage;
			PageId currentPageNumber = rootPageNum;
			PageId saveCurrentPageNumber;
			int i;
			int localNLCount=0;
			int accessLevel = 0;

			// flush access path;

			while(localNLCount != 1){

				accessPath[accessLevel++] = currentPageNumber;

				struct NonLeafNodeInt* nlNode;
				bufMgr->readPage(file, currentPageNumber, currentPage); //- start
				nlNode = (NonLeafNodeInt *)currentPage;
				saveCurrentPageNumber = currentPageNumber;

				if(*(int *)key < nlNode->keyArray[0]){
					if(nlNode->pageNoArray[0] == 5000 && nlNode->level == 1){

						PageId newLeftPageNumber;
						Page *newLeftPage;
						bufMgr->allocPage(file, newLeftPageNumber, newLeftPage);// -----------start

						initializeLeaf((LeafNodeInt *)newLeftPage);

						((LeafNodeInt *)newLeftPage)->keyArray[0] = *(int *)key;
						((LeafNodeInt *)newLeftPage)->ridArray[0] = rid;
						((LeafNodeInt *)newLeftPage)->rightSibPageNo = nlNode->pageNoArray[1];

						nlNode->pageNoArray[0] = newLeftPageNumber;

						//printNode((LeafNodeInt *)newLeftPage);
						bufMgr->unPinPage(file, saveCurrentPageNumber, true); //- start
						bufMgr->unPinPage(file, newLeftPageNumber, true);// ------------------end

						leafOccupancy++;

						return;

					}else if(nlNode->pageNoArray[0] == 5000 && nlNode->level != 1){
						std::cout<< "This is not implemented yet" << "\n";
					}else{
						currentPageNumber = nlNode->pageNoArray[0];
					}

				}else{
					for(i=0; i<INTARRAYNONLEAFSIZE && nlNode->keyArray[i+1] != -1; i++){
						if((*(int *)key > nlNode->keyArray[i]) && (*(int *)key < nlNode->keyArray[i+1])){
							currentPageNumber = nlNode->pageNoArray[i+1];
							break;
						}
					}
					if(i==INTARRAYNONLEAFSIZE){
						currentPageNumber = nlNode->pageNoArray[INTARRAYNONLEAFSIZE-1];
					}else{
						currentPageNumber = nlNode->pageNoArray[i+1];
					}
				}
				localNLCount = nlNode->level;
				bufMgr->unPinPage(file, saveCurrentPageNumber, false);
			}

			// now let us take care of leaf node

			struct LeafNodeInt *lNode;

			bufMgr->readPage(file, currentPageNumber, currentPage); // start -------------->
			accessPath[accessLevel] = currentPageNumber;
			lNode = (LeafNodeInt *)currentPage;

			for(i=0; i < INTARRAYLEAFSIZE && lNode->keyArray[i] != -1; i++);

			if(i == INTARRAYLEAFSIZE){
				// I will make an array first
				int* keyTemp = new int [INTARRAYLEAFSIZE+1];
				RecordId* ridTemp = new RecordId [INTARRAYLEAFSIZE+1];
				int set = 0;
				int j;
				for(i=0,j=0; i<INTARRAYLEAFSIZE+1; i++){
					if((j==INTARRAYLEAFSIZE)||(!set &&*(int *)key < lNode->keyArray[j])){
						keyTemp[i] = *(int *)key;
						ridTemp[i] = rid;
						set = 1;
					}else{
						keyTemp[i] = lNode->keyArray[j];
						ridTemp[i] = lNode->ridArray[j];
						j++;
					}
				}
				// Copy it to first node
				for(i=0;i<=INTARRAYLEAFSIZE/2;i++){
					lNode->keyArray[i] = keyTemp[i];
					lNode->ridArray[i] = ridTemp[i];
				}
				for(int n=i;n<INTARRAYLEAFSIZE;n++){
					lNode->keyArray[n] = -1;
				}

				// Now make a second node
				PageId secondPageNumber;
				Page* tempPage;
				bufMgr->allocPage(file, secondPageNumber, tempPage); //----- start

				initializeLeaf((LeafNodeInt *)tempPage);

				for(int n=i,j=0;n<INTARRAYLEAFSIZE+1;n++,j++){
					((LeafNodeInt *)tempPage)->keyArray[j] = keyTemp[n];
					((LeafNodeInt *)tempPage)->ridArray[j] = ridTemp[n];
				}

				((LeafNodeInt *)tempPage)->rightSibPageNo = lNode->rightSibPageNo;
				lNode->rightSibPageNo = secondPageNumber;

				bufMgr->unPinPage(file, currentPageNumber, true);// ---------end
				bufMgr->unPinPage(file, secondPageNumber, true); // ---------end

				leafOccupancy++;

				delete[] keyTemp;
				delete[] ridTemp;

				// Now send this entry to parent
				addToParent((void *)&(((LeafNodeInt *)tempPage)->keyArray[0]), secondPageNumber, 0, accessLevel-1);

			}else{

				for(i=0; lNode->keyArray[i] != -1 && *(int *)key > lNode->keyArray[i]; i++);

				if(lNode->keyArray[i] == -1){
					lNode->keyArray[i] = *(int *)key;
					lNode->ridArray[i] = rid;
				}else{
					int n;
					for(n=i; lNode->keyArray[n]!=-1; n++);
					while(n!=i){
						lNode->keyArray[n] = lNode->keyArray[n-1];
						lNode->ridArray[n] = lNode->ridArray[n-1];
						n--;
					}
					lNode->keyArray[n] = *(int *)key;
					lNode->ridArray[n] = rid;
				}
				bufMgr->unPinPage(file, currentPageNumber, true); // end <---------------
			}
		}
		/*switch(attributeType){
			case 0 : insertIntegerEntry(key, rid);
				break;
			case 1 : insertDoubleEntry(key, rid);
				break;
			case 2 : insertStringEntry(key, rid);
				break;
		}*/
	}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

	const void BTreeIndex::startScan(const void* lowValParm,
									 const Operator lowOpParm,
									 const void* highValParm,
									 const Operator highOpParm)
	{


		//check if scan is already executing; then end the scan
		if(scanExecuting == true){ // <--- is this needed ?
			endScan();
		}

		lowOp = lowOpParm;
		highOp = highOpParm;

		//check if low operator is GT or GTE; check if high operator is LT or LTE
		if((lowOp != GT && lowOp != GTE) || (highOp != LT && highOp != LTE)){
			throw BadOpcodesException();
		}

		//if everything fine than set executing scan
		scanExecuting = true;

		//set Index of next entry to be scanned in current leaf being scanned as 0
		nextEntry = -1; // <---- Should be -1
	//	startIntScan(lowValParm, highValParm);
		lowValInt = *(int*)lowValParm;
		highValInt = *(int*)highValParm;

		if(highValInt < lowValInt){
			throw BadScanrangeException();
		}

		Page *currentPage;
		PageId currentPageNumber = rootPageNum;

		int i;
		int localNLCount=0;

		// First loop in the non-leaf nodes

		while(localNLCount != 1){

			struct NonLeafNodeInt* nlNode;
			bufMgr->readPage(file, currentPageNumber, currentPage); //- start
			nlNode = (NonLeafNodeInt *)currentPage;
			bufMgr->unPinPage(file, currentPageNumber, false);
			if(lowValInt < nlNode->keyArray[0]){
				if(nlNode->pageNoArray[0] == 5000){
					currentPageNumber = nlNode->pageNoArray[1];
				}else{
					currentPageNumber = nlNode->pageNoArray[0];
				}
			}else{
				for(i=0; i<INTARRAYNONLEAFSIZE && nlNode->keyArray[i+1] != -1; i++){
					if((lowValInt > nlNode->keyArray[i]) && (lowValInt < nlNode->keyArray[i+1])){
						currentPageNumber = nlNode->pageNoArray[i+1];
						break;
					}
				}
				if(i==INTARRAYNONLEAFSIZE){
					currentPageNumber = nlNode->pageNoArray[INTARRAYNONLEAFSIZE-1];
				}else{
					currentPageNumber = nlNode->pageNoArray[i+1];
				}
			}

			localNLCount = nlNode->level;
		}

		// Now we have the leaf page in currentPageNumber

		int got = 0;

		while(got != 1){
			struct LeafNodeInt* lNode;
			bufMgr->readPage(file, currentPageNumber, currentPage); //- start
			lNode = (LeafNodeInt *)currentPage;
			for(int i=0;i<INTARRAYLEAFSIZE && lNode->keyArray[i]!=-1 ;i++){
				if(lowOp == GTE){
					if(lNode->keyArray[i] == lowValInt){
						currentPageNum = currentPageNumber;
						nextEntry = i;
						got = 1;
						break;
					}
				}else{
					if(lNode->keyArray[i] > lowValInt){
						currentPageNum = currentPageNumber;
						nextEntry = i;
						got = 1;
						break;
					}
				}
			}
			bufMgr->unPinPage(file, currentPageNumber, false);
			currentPageNumber = lNode->rightSibPageNo;
		}


	}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

	const void BTreeIndex::scanNext(RecordId& outRid)
	{

		LeafNodeInt *currLeafNode;

		//cout << "scanNext Start\n";
		if (scanExecuting==false)
		{
			throw ScanNotInitializedException();
		}
		if (currentPageNum == Page::INVALID_NUMBER)
		{
			throw IndexScanCompletedException();

		}
		bufMgr->readPage(file, currentPageNum, currentPageData);
		currLeafNode = (LeafNodeInt *)currentPageData;

		//check if scan is completed
		if (currentPageNum == Page::INVALID_NUMBER || currLeafNode->keyArray[nextEntry] > highValInt || (currLeafNode->keyArray[nextEntry] == highValInt && highOp == LT))
		{
			throw IndexScanCompletedException();
		}

		outRid = currLeafNode->ridArray[nextEntry];
		nextEntry++;


		if(nextEntry >= INTARRAYLEAFSIZE || currLeafNode->keyArray[nextEntry] == -1)
		{
			bufMgr->unPinPage(file, currentPageNum, false);
			currentPageNum = currLeafNode->rightSibPageNo;
			nextEntry=0;
			return;
		}else{
			bufMgr->unPinPage(file, currentPageNum, false);
		}


		/*switch(attributeType)
		{
			case 0:

				currLeafNode = (LeafNodeInt *)currentPageData;

				//check if scan is completed
				if (currentPageNum == Page::INVALID_NUMBER || currLeafNode->keyArray[nextEntry] > highValInt || (currLeafNode->keyArray[nextEntry] == highValInt && highOp == LT))
				{
					throw IndexScanCompletedException();
				}

				outRid = currLeafNode->ridArray[nextEntry];
				nextEntry++;


				if(nextEntry >= INTARRAYLEAFSIZE || currLeafNode->keyArray[nextEntry] == -1)
				{
					bufMgr->unPinPage(file, currentPageNum, false);
					currentPageNum = currLeafNode->rightSibPageNo;
					nextEntry=0;
					return;
				}else{
					bufMgr->unPinPage(file, currentPageNum, false);
				}

				break;

			case 1:

				currLeafNodeD = (LeafNodeDouble *)currentPageData;
				//check if scan is completed
				if (currentPageNum == Page::INVALID_NUMBER || currLeafNodeD->keyArray[nextEntry] > highValDouble || (currLeafNodeD->keyArray[nextEntry] == highValDouble && highOp == LT))
				{
					throw IndexScanCompletedException();
				}

				outRid = currLeafNodeD->ridArray[nextEntry];
				nextEntry++;

				if(nextEntry >= DOUBLEARRAYLEAFSIZE || currLeafNodeD->keyArray[nextEntry] == -1)
				{
					bufMgr->unPinPage(file, currentPageNum, false);
					currentPageNum = currLeafNodeD->rightSibPageNo;
					nextEntry=0;
					return;
				}else{
					bufMgr->unPinPage(file, currentPageNum, false);
				}

				break;

			case 2:

				currLeafNodeS = (LeafNodeString *)currentPageData;
				//check if scan is completed
				if (currentPageNum == Page::INVALID_NUMBER || strncmp(currLeafNodeS->keyArray[nextEntry], highValString.c_str(), 10) > 0 || (strncmp(currLeafNodeS->keyArray[nextEntry], highValString.c_str(), 10) == 0 && highOp == LT))
				{
					throw IndexScanCompletedException();
				}

				outRid = currLeafNodeS->ridArray[nextEntry];
				nextEntry++;

				if(nextEntry >= STRINGARRAYLEAFSIZE || currLeafNodeS->keyArray[nextEntry][0] == '\0')
				{
					bufMgr->unPinPage(file, currentPageNum, false);
					currentPageNum = currLeafNodeS->rightSibPageNo;
					nextEntry=0;
					return;
				}else{
					bufMgr->unPinPage(file, currentPageNum, false);
				}

				break;

		}*/
	}


// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
	const void BTreeIndex::endScan()
	{

		//check if scan is even started or not
		if(scanExecuting == false)
		{
			throw ScanNotInitializedException();
		}

//unpin all the pages pinned for the purpose of scan
		try
		{
			bufMgr->unPinPage(file, currentPageNum, false);
		}
		catch (BadgerDbException pnpe)
		{
		}

		nextEntry = 0;
		currentPageNum = 0;
		currentPageData = NULL;
		scanExecuting = false;
	}


}
