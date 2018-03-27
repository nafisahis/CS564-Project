/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
}

void BufMgr::advanceClock()
{
	clockHand = (clockHand+1)%numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
    //In process
    bool flag = false;
    for (FrameId i = 0; i < numBufs; i++) 
  {
	if(bufDescTable[i].pinCnt==0)
	{ flag = true;
	  break;
	}
  }
  if(!flag)
	throw BufferExceededException();
	
     
     //FrameId allocatedFrame;
    while(true){
	std::cout<<"Inside while of allocBuf\n"; 
       if(bufDescTable[clockHand].valid) {
           if(bufDescTable[clockHand].refbit){
		std::cout<<"Inside refbit is false\n";
               //clear refbit.
               bufDescTable[clockHand].refbit = false;
               //call advance pointer function.
		std::cout<<"Just before the advance clock function\n";
               advanceClock();
		std::cout<<"After the 1st advance clock function\n";
               break;
           }else{
               if(bufDescTable[clockHand].pinCnt == 0){
		   std::cout<<"Inside pincount = 0\n";
			File *file;
        		PageId pageid;
        		Page page;
                    if(bufDescTable[clockHand].dirty){
                        //flush page to disk
			

			file = bufDescTable[clockHand].file;
			pageid = bufDescTable[clockHand].pageNo;
			std::cout<<"Inside allocbufout\n";
			page = file->readPage(pageid);
			std::cout<<"Inside allocbuf\n";
			file->writePage(page);		
			
                    }
                   //call set() on the frame. how to do this?
                   frame = clockHand;
		   bufDescTable[clockHand].frameNo = frame;
		   hashTable->remove(file,pageid);
		   bufPool[clockHand] = page;
                   return;
                   //use the frame.
               }else{
                   //call advance pointer function. how to do this?
                   advanceClock();
                   break;
               }
           }
       }else{
	std::cout<<"Inside not valid\n";
            //call set() on the frame.
          //allocatedFrame = clockHand;
	
	File *file1;
        //const PageId pageid1;
        Page page1;
	  frame = clockHand;
	  file1 = bufDescTable[clockHand].file;
	  const PageId pageid1 = bufDescTable[clockHand].pageNo;
	std::cout<<pageid1;
	std::cout<<"Before reading page\n";
	
	  page1 = file1->readPage(pageid1);
	std::cout<<"After  reading page\n";
	  bufDescTable[clockHand].frameNo = frame; 
	   bufPool[clockHand] = page1;
           return;
           //use the frame.
       }
    }
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
FrameId frameid;

try {
	hashTable->lookup(file,pageNo,frameid);
	bufDescTable[frameid].refbit = true;
	bufDescTable[frameid].pinCnt++;
	page = &bufPool[frameid];
	

}
catch(HashNotFoundException e){
	allocBuf(frameid);
	bufPool[frameid] = file->readPage(pageNo);
	hashTable->insert(file,pageNo,frameid);
	bufDescTable[frameid].Set(file,pageNo);
	page = &bufPool[frameid];
}

}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frameid;
try {
	hashTable->lookup(file,pageNo,frameid);

	if(bufDescTable[frameid].pinCnt!=0){
		bufDescTable[frameid].pinCnt--;
		if(bufDescTable[frameid].dirty)
			bufDescTable[frameid].dirty = false;
	}
	else
		throw PageNotPinnedException(file->filename(),pageNo,frameid);

}
catch(HashNotFoundException e){
}
}

void BufMgr::flushFile(const File* file) 
{
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	Page p;
	p = file->allocatePage();
	
	page = &p;
	//std::cout<<"Inside allocPage1\n";
	FrameId frame;
	allocBuf(frame);
	//std::cout<<"Inside allocPage\n";
	pageNo = page->page_number();
	
	hashTable->insert(file,pageNo,frame);
	bufDescTable[frame].Set(file,pageNo);
	
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
