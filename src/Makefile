JFLAGS =
JC = javac

.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $.*/*.java

CLASSES = \
	$(wildcard Core/*.java) \
	$(wildcard InvertedIndexExample/*.java) \
	$(wildcard Job/*.java) \
	$(wildcard Master/*.java) \
	$(wildcard Messages/*.java) \
	$(wildcard Participant/*.java) \
	$(wildcard WordCountJobExample/*.java) \
	
default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) ./*/*.class