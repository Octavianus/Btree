#
# Makefile for CS564 Minibase project.  Needs GNU make.
#
# Define DEBUGREL for some kind of debugging output (not from us, from
# the original Minibase implementors.)
#
# Warning: make depend overwrites this file.

.PHONY: depend clean backup setup

MAIN=btree

MINIBASE = ..

CC=g++

CFLAGS= -DUNIX -Wall -g

INCLUDES = -I${MINIBASE}/include -I.

# LFLAGS= -L${MINIBASE}/lib  -liberty -lbtree -lm

# you need to change this 

SRCS =  main.C btree_driver.C btfile.C btindex_page.C \
	btleaf_page.C buf.C db.C new_error.C key.C \
	btreefilescan.C system_defs.C page.C sorted_page.C hfpage.C

OBJS = $(SRCS:.C=.o)

$(MAIN):  $(OBJS)
	 $(CC) $(CFLAGS) $(INCLUDES) $(OBJS) -o $(MAIN)

.C.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $<

depend: $(SRCS)
	makedepend $(INCLUDES) $^

clean:
	mv buf.o buf.obak
	mv hfpage.o hfpage.obak
	rm -f *.o *~ $(MAIN)
	mv buf.obak buf.o
	mv hfpage.obak hfpage.o

backup:
	-mkdir bak
	cp Makefile *.[Ch] bak

# Grab the sources for a user who has only the makefile
setup:
	/bin/cp -i $(MINIBASE)/src/*.[Ch] .

# DO NOT DELETE THIS LINE -- make depend needs it 
