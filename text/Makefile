FILE=BP

#SVGFILES = $(shell ls *.svg)
#SVGPDF = $(patsubst %.svg, %.pdf, $(SVGFILES))

all: pdf # $(FILE).pdf

# $(FILE).pdf: # *.tex Makefile
#	make pdf
# @while grep Rerun $(FILE).log >/dev/null 2>&1 ; do \
# 	make pdf; \
# done

pdf: # fig
	pdflatex $(FILE)

# fig: $(SVGPDF)

# $(SVGPDF): %.pdf: %.svg
# 	inkscape -z --export-pdf=$@ --export-area-drawing --export-dpi=300 $<

final:
	mv $(FILE).tex matej-krejci-bp-2014.pdf

clean:
	rm -f *~ *.aux *.log *.out *.toc
#	rm -f $(SVGPDF)

distclean:
	make clean
	rm -f $(FILE).pdf
