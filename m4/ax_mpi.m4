dnl @synopsis ACX_MPI([ACTION-IF-FOUND[, ACTION-IF-NOT-FOUND]])
dnl
dnl @summary figure out how to compile/link code with MPI
dnl
dnl This macro tries to find out how to compile programs that use MPI
dnl (Message Passing Interface), a standard API for parallel process
dnl communication (see http://www-unix.mcs.anl.gov/mpi/)
dnl
dnl On success, it sets the MPICC, MPICXX, or MPIF77 output variable to
dnl the name of the MPI compiler, depending upon the current language.
dnl (This may just be $CC/$CXX/$F77, but is more often something like
dnl mpicc/mpiCC/mpif77.) It also sets MPILIBS to any libraries that are
dnl needed for linking MPI (e.g. -lmpi, if a special
dnl MPICC/MPICXX/MPIF77 was not found).
dnl
dnl If you want to compile everything with MPI, you should set:
dnl
dnl     CC="$MPICC" #OR# CXX="$MPICXX" #OR# F77="$MPIF77"
dnl     LIBS="$MPILIBS $LIBS"
dnl
dnl NOTE: The above assumes that you will use $CC (or whatever) for
dnl linking as well as for compiling. (This is the default for automake
dnl and most Makefiles.)
dnl
dnl The user can force a particular library/compiler by setting the
dnl MPICC/MPICXX/MPIF77 and/or MPILIBS environment variables.
dnl
dnl ACTION-IF-FOUND is a list of shell commands to run if an MPI
dnl library is found, and ACTION-IF-NOT-FOUND is a list of commands to
dnl run it if it is not found. If ACTION-IF-FOUND is not specified, the
dnl default action will define HAVE_MPI.
dnl
dnl @category InstalledPackages
dnl @author Steven G. Johnson <stevenj@alum.mit.edu>
dnl @author Julian Cummings <cummings@cacr.caltech.edu>
dnl @version 2006-10-13
dnl @license GPLWithACException
dnl

dnl
dnl The "GPLWithACException" license applies to this file only. License text
dnl is reproduced below in accordance with the license terms.
dnl
dnl                         GPLWithACException
dnl
dnl This program is free software; you can redistribute it and/or modify it
dnl under the terms of the GNU General Public License as published by the
dnl Free Software Foundation. As a special exception, the respective Autoconf
dnl Macro's copyright owner gives unlimited permission to copy, distribute and
dnl modify the configure scripts that are the output of Autoconf when processing
dnl the Macro. You need not follow the terms of the GNU General Public License
dnl when using or distributing such scripts.
dnl
dnl                     GNU GENERAL PUBLIC LICENSE
dnl                       Version 3, 29 June 2007
dnl 
dnl  Copyright (C) 2007 Free Software Foundation, Inc. <http://fsf.org/>
dnl  Everyone is permitted to copy and distribute verbatim copies
dnl  of this license document, but changing it is not allowed.
dnl 
dnl                             Preamble
dnl 
dnl   The GNU General Public License is a free, copyleft license for
dnl software and other kinds of works.
dnl 
dnl   The licenses for most software and other practical works are designed
dnl to take away your freedom to share and change the works.  By contrast,
dnl the GNU General Public License is intended to guarantee your freedom to
dnl share and change all versions of a program--to make sure it remains free
dnl software for all its users.  We, the Free Software Foundation, use the
dnl GNU General Public License for most of our software; it applies also to
dnl any other work released this way by its authors.  You can apply it to
dnl your programs, too.
dnl 
dnl   When we speak of free software, we are referring to freedom, not
dnl price.  Our General Public Licenses are designed to make sure that you
dnl have the freedom to distribute copies of free software (and charge for
dnl them if you wish), that you receive source code or can get it if you
dnl want it, that you can change the software or use pieces of it in new
dnl free programs, and that you know you can do these things.
dnl 
dnl   To protect your rights, we need to prevent others from denying you
dnl these rights or asking you to surrender the rights.  Therefore, you have
dnl certain responsibilities if you distribute copies of the software, or if
dnl you modify it: responsibilities to respect the freedom of others.
dnl 
dnl   For example, if you distribute copies of such a program, whether
dnl gratis or for a fee, you must pass on to the recipients the same
dnl freedoms that you received.  You must make sure that they, too, receive
dnl or can get the source code.  And you must show them these terms so they
dnl know their rights.
dnl 
dnl   Developers that use the GNU GPL protect your rights with two steps:
dnl (1) assert copyright on the software, and (2) offer you this License
dnl giving you legal permission to copy, distribute and/or modify it.
dnl 
dnl   For the developers' and authors' protection, the GPL clearly explains
dnl that there is no warranty for this free software.  For both users' and
dnl authors' sake, the GPL requires that modified versions be marked as
dnl changed, so that their problems will not be attributed erroneously to
dnl authors of previous versions.
dnl 
dnl   Some devices are designed to deny users access to install or run
dnl modified versions of the software inside them, although the manufacturer
dnl can do so.  This is fundamentally incompatible with the aim of
dnl protecting users' freedom to change the software.  The systematic
dnl pattern of such abuse occurs in the area of products for individuals to
dnl use, which is precisely where it is most unacceptable.  Therefore, we
dnl have designed this version of the GPL to prohibit the practice for those
dnl products.  If such problems arise substantially in other domains, we
dnl stand ready to extend this provision to those domains in future versions
dnl of the GPL, as needed to protect the freedom of users.
dnl 
dnl   Finally, every program is threatened constantly by software patents.
dnl States should not allow patents to restrict development and use of
dnl software on general-purpose computers, but in those that do, we wish to
dnl avoid the special danger that patents applied to a free program could
dnl make it effectively proprietary.  To prevent this, the GPL assures that
dnl patents cannot be used to render the program non-free.
dnl 
dnl   The precise terms and conditions for copying, distribution and
dnl modification follow.
dnl 
dnl                        TERMS AND CONDITIONS
dnl 
dnl   0. Definitions.
dnl 
dnl   "This License" refers to version 3 of the GNU General Public License.
dnl 
dnl   "Copyright" also means copyright-like laws that apply to other kinds of
dnl works, such as semiconductor masks.
dnl 
dnl   "The Program" refers to any copyrightable work licensed under this
dnl License.  Each licensee is addressed as "you".  "Licensees" and
dnl "recipients" may be individuals or organizations.
dnl 
dnl   To "modify" a work means to copy from or adapt all or part of the work
dnl in a fashion requiring copyright permission, other than the making of an
dnl exact copy.  The resulting work is called a "modified version" of the
dnl earlier work or a work "based on" the earlier work.
dnl 
dnl   A "covered work" means either the unmodified Program or a work based
dnl on the Program.
dnl 
dnl   To "propagate" a work means to do anything with it that, without
dnl permission, would make you directly or secondarily liable for
dnl infringement under applicable copyright law, except executing it on a
dnl computer or modifying a private copy.  Propagation includes copying,
dnl distribution (with or without modification), making available to the
dnl public, and in some countries other activities as well.
dnl 
dnl   To "convey" a work means any kind of propagation that enables other
dnl parties to make or receive copies.  Mere interaction with a user through
dnl a computer network, with no transfer of a copy, is not conveying.
dnl 
dnl   An interactive user interface displays "Appropriate Legal Notices"
dnl to the extent that it includes a convenient and prominently visible
dnl feature that (1) displays an appropriate copyright notice, and (2)
dnl tells the user that there is no warranty for the work (except to the
dnl extent that warranties are provided), that licensees may convey the
dnl work under this License, and how to view a copy of this License.  If
dnl the interface presents a list of user commands or options, such as a
dnl menu, a prominent item in the list meets this criterion.
dnl 
dnl   1. Source Code.
dnl 
dnl   The "source code" for a work means the preferred form of the work
dnl for making modifications to it.  "Object code" means any non-source
dnl form of a work.
dnl 
dnl   A "Standard Interface" means an interface that either is an official
dnl standard defined by a recognized standards body, or, in the case of
dnl interfaces specified for a particular programming language, one that
dnl is widely used among developers working in that language.
dnl 
dnl   The "System Libraries" of an executable work include anything, other
dnl than the work as a whole, that (a) is included in the normal form of
dnl packaging a Major Component, but which is not part of that Major
dnl Component, and (b) serves only to enable use of the work with that
dnl Major Component, or to implement a Standard Interface for which an
dnl implementation is available to the public in source code form.  A
dnl "Major Component", in this context, means a major essential component
dnl (kernel, window system, and so on) of the specific operating system
dnl (if any) on which the executable work runs, or a compiler used to
dnl produce the work, or an object code interpreter used to run it.
dnl 
dnl   The "Corresponding Source" for a work in object code form means all
dnl the source code needed to generate, install, and (for an executable
dnl work) run the object code and to modify the work, including scripts to
dnl control those activities.  However, it does not include the work's
dnl System Libraries, or general-purpose tools or generally available free
dnl programs which are used unmodified in performing those activities but
dnl which are not part of the work.  For example, Corresponding Source
dnl includes interface definition files associated with source files for
dnl the work, and the source code for shared libraries and dynamically
dnl linked subprograms that the work is specifically designed to require,
dnl such as by intimate data communication or control flow between those
dnl subprograms and other parts of the work.
dnl 
dnl   The Corresponding Source need not include anything that users
dnl can regenerate automatically from other parts of the Corresponding
dnl Source.
dnl 
dnl   The Corresponding Source for a work in source code form is that
dnl same work.
dnl 
dnl   2. Basic Permissions.
dnl 
dnl   All rights granted under this License are granted for the term of
dnl copyright on the Program, and are irrevocable provided the stated
dnl conditions are met.  This License explicitly affirms your unlimited
dnl permission to run the unmodified Program.  The output from running a
dnl covered work is covered by this License only if the output, given its
dnl content, constitutes a covered work.  This License acknowledges your
dnl rights of fair use or other equivalent, as provided by copyright law.
dnl 
dnl   You may make, run and propagate covered works that you do not
dnl convey, without conditions so long as your license otherwise remains
dnl in force.  You may convey covered works to others for the sole purpose
dnl of having them make modifications exclusively for you, or provide you
dnl with facilities for running those works, provided that you comply with
dnl the terms of this License in conveying all material for which you do
dnl not control copyright.  Those thus making or running the covered works
dnl for you must do so exclusively on your behalf, under your direction
dnl and control, on terms that prohibit them from making any copies of
dnl your copyrighted material outside their relationship with you.
dnl 
dnl   Conveying under any other circumstances is permitted solely under
dnl the conditions stated below.  Sublicensing is not allowed; section 10
dnl makes it unnecessary.
dnl 
dnl   3. Protecting Users' Legal Rights From Anti-Circumvention Law.
dnl 
dnl   No covered work shall be deemed part of an effective technological
dnl measure under any applicable law fulfilling obligations under article
dnl 11 of the WIPO copyright treaty adopted on 20 December 1996, or
dnl similar laws prohibiting or restricting circumvention of such
dnl measures.
dnl 
dnl   When you convey a covered work, you waive any legal power to forbid
dnl circumvention of technological measures to the extent such circumvention
dnl is effected by exercising rights under this License with respect to
dnl the covered work, and you disclaim any intention to limit operation or
dnl modification of the work as a means of enforcing, against the work's
dnl users, your or third parties' legal rights to forbid circumvention of
dnl technological measures.
dnl 
dnl   4. Conveying Verbatim Copies.
dnl 
dnl   You may convey verbatim copies of the Program's source code as you
dnl receive it, in any medium, provided that you conspicuously and
dnl appropriately publish on each copy an appropriate copyright notice;
dnl keep intact all notices stating that this License and any
dnl non-permissive terms added in accord with section 7 apply to the code;
dnl keep intact all notices of the absence of any warranty; and give all
dnl recipients a copy of this License along with the Program.
dnl 
dnl   You may charge any price or no price for each copy that you convey,
dnl and you may offer support or warranty protection for a fee.
dnl 
dnl   5. Conveying Modified Source Versions.
dnl 
dnl   You may convey a work based on the Program, or the modifications to
dnl produce it from the Program, in the form of source code under the
dnl terms of section 4, provided that you also meet all of these conditions:
dnl 
dnl     a) The work must carry prominent notices stating that you modified
dnl     it, and giving a relevant date.
dnl 
dnl     b) The work must carry prominent notices stating that it is
dnl     released under this License and any conditions added under section
dnl     7.  This requirement modifies the requirement in section 4 to
dnl     "keep intact all notices".
dnl 
dnl     c) You must license the entire work, as a whole, under this
dnl     License to anyone who comes into possession of a copy.  This
dnl     License will therefore apply, along with any applicable section 7
dnl     additional terms, to the whole of the work, and all its parts,
dnl     regardless of how they are packaged.  This License gives no
dnl     permission to license the work in any other way, but it does not
dnl     invalidate such permission if you have separately received it.
dnl 
dnl     d) If the work has interactive user interfaces, each must display
dnl     Appropriate Legal Notices; however, if the Program has interactive
dnl     interfaces that do not display Appropriate Legal Notices, your
dnl     work need not make them do so.
dnl 
dnl   A compilation of a covered work with other separate and independent
dnl works, which are not by their nature extensions of the covered work,
dnl and which are not combined with it such as to form a larger program,
dnl in or on a volume of a storage or distribution medium, is called an
dnl "aggregate" if the compilation and its resulting copyright are not
dnl used to limit the access or legal rights of the compilation's users
dnl beyond what the individual works permit.  Inclusion of a covered work
dnl in an aggregate does not cause this License to apply to the other
dnl parts of the aggregate.
dnl 
dnl   6. Conveying Non-Source Forms.
dnl 
dnl   You may convey a covered work in object code form under the terms
dnl of sections 4 and 5, provided that you also convey the
dnl machine-readable Corresponding Source under the terms of this License,
dnl in one of these ways:
dnl 
dnl     a) Convey the object code in, or embodied in, a physical product
dnl     (including a physical distribution medium), accompanied by the
dnl     Corresponding Source fixed on a durable physical medium
dnl     customarily used for software interchange.
dnl 
dnl     b) Convey the object code in, or embodied in, a physical product
dnl     (including a physical distribution medium), accompanied by a
dnl     written offer, valid for at least three years and valid for as
dnl     long as you offer spare parts or customer support for that product
dnl     model, to give anyone who possesses the object code either (1) a
dnl     copy of the Corresponding Source for all the software in the
dnl     product that is covered by this License, on a durable physical
dnl     medium customarily used for software interchange, for a price no
dnl     more than your reasonable cost of physically performing this
dnl     conveying of source, or (2) access to copy the
dnl     Corresponding Source from a network server at no charge.
dnl 
dnl     c) Convey individual copies of the object code with a copy of the
dnl     written offer to provide the Corresponding Source.  This
dnl     alternative is allowed only occasionally and noncommercially, and
dnl     only if you received the object code with such an offer, in accord
dnl     with subsection 6b.
dnl 
dnl     d) Convey the object code by offering access from a designated
dnl     place (gratis or for a charge), and offer equivalent access to the
dnl     Corresponding Source in the same way through the same place at no
dnl     further charge.  You need not require recipients to copy the
dnl     Corresponding Source along with the object code.  If the place to
dnl     copy the object code is a network server, the Corresponding Source
dnl     may be on a different server (operated by you or a third party)
dnl     that supports equivalent copying facilities, provided you maintain
dnl     clear directions next to the object code saying where to find the
dnl     Corresponding Source.  Regardless of what server hosts the
dnl     Corresponding Source, you remain obligated to ensure that it is
dnl     available for as long as needed to satisfy these requirements.
dnl 
dnl     e) Convey the object code using peer-to-peer transmission, provided
dnl     you inform other peers where the object code and Corresponding
dnl     Source of the work are being offered to the general public at no
dnl     charge under subsection 6d.
dnl 
dnl   A separable portion of the object code, whose source code is excluded
dnl from the Corresponding Source as a System Library, need not be
dnl included in conveying the object code work.
dnl 
dnl   A "User Product" is either (1) a "consumer product", which means any
dnl tangible personal property which is normally used for personal, family,
dnl or household purposes, or (2) anything designed or sold for incorporation
dnl into a dwelling.  In determining whether a product is a consumer product,
dnl doubtful cases shall be resolved in favor of coverage.  For a particular
dnl product received by a particular user, "normally used" refers to a
dnl typical or common use of that class of product, regardless of the status
dnl of the particular user or of the way in which the particular user
dnl actually uses, or expects or is expected to use, the product.  A product
dnl is a consumer product regardless of whether the product has substantial
dnl commercial, industrial or non-consumer uses, unless such uses represent
dnl the only significant mode of use of the product.
dnl 
dnl   "Installation Information" for a User Product means any methods,
dnl procedures, authorization keys, or other information required to install
dnl and execute modified versions of a covered work in that User Product from
dnl a modified version of its Corresponding Source.  The information must
dnl suffice to ensure that the continued functioning of the modified object
dnl code is in no case prevented or interfered with solely because
dnl modification has been made.
dnl 
dnl   If you convey an object code work under this section in, or with, or
dnl specifically for use in, a User Product, and the conveying occurs as
dnl part of a transaction in which the right of possession and use of the
dnl User Product is transferred to the recipient in perpetuity or for a
dnl fixed term (regardless of how the transaction is characterized), the
dnl Corresponding Source conveyed under this section must be accompanied
dnl by the Installation Information.  But this requirement does not apply
dnl if neither you nor any third party retains the ability to install
dnl modified object code on the User Product (for example, the work has
dnl been installed in ROM).
dnl 
dnl   The requirement to provide Installation Information does not include a
dnl requirement to continue to provide support service, warranty, or updates
dnl for a work that has been modified or installed by the recipient, or for
dnl the User Product in which it has been modified or installed.  Access to a
dnl network may be denied when the modification itself materially and
dnl adversely affects the operation of the network or violates the rules and
dnl protocols for communication across the network.
dnl 
dnl   Corresponding Source conveyed, and Installation Information provided,
dnl in accord with this section must be in a format that is publicly
dnl documented (and with an implementation available to the public in
dnl source code form), and must require no special password or key for
dnl unpacking, reading or copying.
dnl 
dnl   7. Additional Terms.
dnl 
dnl   "Additional permissions" are terms that supplement the terms of this
dnl License by making exceptions from one or more of its conditions.
dnl Additional permissions that are applicable to the entire Program shall
dnl be treated as though they were included in this License, to the extent
dnl that they are valid under applicable law.  If additional permissions
dnl apply only to part of the Program, that part may be used separately
dnl under those permissions, but the entire Program remains governed by
dnl this License without regard to the additional permissions.
dnl 
dnl   When you convey a copy of a covered work, you may at your option
dnl remove any additional permissions from that copy, or from any part of
dnl it.  (Additional permissions may be written to require their own
dnl removal in certain cases when you modify the work.)  You may place
dnl additional permissions on material, added by you to a covered work,
dnl for which you have or can give appropriate copyright permission.
dnl 
dnl   Notwithstanding any other provision of this License, for material you
dnl add to a covered work, you may (if authorized by the copyright holders of
dnl that material) supplement the terms of this License with terms:
dnl 
dnl     a) Disclaiming warranty or limiting liability differently from the
dnl     terms of sections 15 and 16 of this License; or
dnl 
dnl     b) Requiring preservation of specified reasonable legal notices or
dnl     author attributions in that material or in the Appropriate Legal
dnl     Notices displayed by works containing it; or
dnl 
dnl     c) Prohibiting misrepresentation of the origin of that material, or
dnl     requiring that modified versions of such material be marked in
dnl     reasonable ways as different from the original version; or
dnl 
dnl     d) Limiting the use for publicity purposes of names of licensors or
dnl     authors of the material; or
dnl 
dnl     e) Declining to grant rights under trademark law for use of some
dnl     trade names, trademarks, or service marks; or
dnl 
dnl     f) Requiring indemnification of licensors and authors of that
dnl     material by anyone who conveys the material (or modified versions of
dnl     it) with contractual assumptions of liability to the recipient, for
dnl     any liability that these contractual assumptions directly impose on
dnl     those licensors and authors.
dnl 
dnl   All other non-permissive additional terms are considered "further
dnl restrictions" within the meaning of section 10.  If the Program as you
dnl received it, or any part of it, contains a notice stating that it is
dnl governed by this License along with a term that is a further
dnl restriction, you may remove that term.  If a license document contains
dnl a further restriction but permits relicensing or conveying under this
dnl License, you may add to a covered work material governed by the terms
dnl of that license document, provided that the further restriction does
dnl not survive such relicensing or conveying.
dnl 
dnl   If you add terms to a covered work in accord with this section, you
dnl must place, in the relevant source files, a statement of the
dnl additional terms that apply to those files, or a notice indicating
dnl where to find the applicable terms.
dnl 
dnl   Additional terms, permissive or non-permissive, may be stated in the
dnl form of a separately written license, or stated as exceptions;
dnl the above requirements apply either way.
dnl 
dnl   8. Termination.
dnl 
dnl   You may not propagate or modify a covered work except as expressly
dnl provided under this License.  Any attempt otherwise to propagate or
dnl modify it is void, and will automatically terminate your rights under
dnl this License (including any patent licenses granted under the third
dnl paragraph of section 11).
dnl 
dnl   However, if you cease all violation of this License, then your
dnl license from a particular copyright holder is reinstated (a)
dnl provisionally, unless and until the copyright holder explicitly and
dnl finally terminates your license, and (b) permanently, if the copyright
dnl holder fails to notify you of the violation by some reasonable means
dnl prior to 60 days after the cessation.
dnl 
dnl   Moreover, your license from a particular copyright holder is
dnl reinstated permanently if the copyright holder notifies you of the
dnl violation by some reasonable means, this is the first time you have
dnl received notice of violation of this License (for any work) from that
dnl copyright holder, and you cure the violation prior to 30 days after
dnl your receipt of the notice.
dnl 
dnl   Termination of your rights under this section does not terminate the
dnl licenses of parties who have received copies or rights from you under
dnl this License.  If your rights have been terminated and not permanently
dnl reinstated, you do not qualify to receive new licenses for the same
dnl material under section 10.
dnl 
dnl   9. Acceptance Not Required for Having Copies.
dnl 
dnl   You are not required to accept this License in order to receive or
dnl run a copy of the Program.  Ancillary propagation of a covered work
dnl occurring solely as a consequence of using peer-to-peer transmission
dnl to receive a copy likewise does not require acceptance.  However,
dnl nothing other than this License grants you permission to propagate or
dnl modify any covered work.  These actions infringe copyright if you do
dnl not accept this License.  Therefore, by modifying or propagating a
dnl covered work, you indicate your acceptance of this License to do so.
dnl 
dnl   10. Automatic Licensing of Downstream Recipients.
dnl 
dnl   Each time you convey a covered work, the recipient automatically
dnl receives a license from the original licensors, to run, modify and
dnl propagate that work, subject to this License.  You are not responsible
dnl for enforcing compliance by third parties with this License.
dnl 
dnl   An "entity transaction" is a transaction transferring control of an
dnl organization, or substantially all assets of one, or subdividing an
dnl organization, or merging organizations.  If propagation of a covered
dnl work results from an entity transaction, each party to that
dnl transaction who receives a copy of the work also receives whatever
dnl licenses to the work the party's predecessor in interest had or could
dnl give under the previous paragraph, plus a right to possession of the
dnl Corresponding Source of the work from the predecessor in interest, if
dnl the predecessor has it or can get it with reasonable efforts.
dnl 
dnl   You may not impose any further restrictions on the exercise of the
dnl rights granted or affirmed under this License.  For example, you may
dnl not impose a license fee, royalty, or other charge for exercise of
dnl rights granted under this License, and you may not initiate litigation
dnl (including a cross-claim or counterclaim in a lawsuit) alleging that
dnl any patent claim is infringed by making, using, selling, offering for
dnl sale, or importing the Program or any portion of it.
dnl 
dnl   11. Patents.
dnl 
dnl   A "contributor" is a copyright holder who authorizes use under this
dnl License of the Program or a work on which the Program is based.  The
dnl work thus licensed is called the contributor's "contributor version".
dnl 
dnl   A contributor's "essential patent claims" are all patent claims
dnl owned or controlled by the contributor, whether already acquired or
dnl hereafter acquired, that would be infringed by some manner, permitted
dnl by this License, of making, using, or selling its contributor version,
dnl but do not include claims that would be infringed only as a
dnl consequence of further modification of the contributor version.  For
dnl purposes of this definition, "control" includes the right to grant
dnl patent sublicenses in a manner consistent with the requirements of
dnl this License.
dnl 
dnl   Each contributor grants you a non-exclusive, worldwide, royalty-free
dnl patent license under the contributor's essential patent claims, to
dnl make, use, sell, offer for sale, import and otherwise run, modify and
dnl propagate the contents of its contributor version.
dnl 
dnl   In the following three paragraphs, a "patent license" is any express
dnl agreement or commitment, however denominated, not to enforce a patent
dnl (such as an express permission to practice a patent or covenant not to
dnl sue for patent infringement).  To "grant" such a patent license to a
dnl party means to make such an agreement or commitment not to enforce a
dnl patent against the party.
dnl 
dnl   If you convey a covered work, knowingly relying on a patent license,
dnl and the Corresponding Source of the work is not available for anyone
dnl to copy, free of charge and under the terms of this License, through a
dnl publicly available network server or other readily accessible means,
dnl then you must either (1) cause the Corresponding Source to be so
dnl available, or (2) arrange to deprive yourself of the benefit of the
dnl patent license for this particular work, or (3) arrange, in a manner
dnl consistent with the requirements of this License, to extend the patent
dnl license to downstream recipients.  "Knowingly relying" means you have
dnl actual knowledge that, but for the patent license, your conveying the
dnl covered work in a country, or your recipient's use of the covered work
dnl in a country, would infringe one or more identifiable patents in that
dnl country that you have reason to believe are valid.
dnl 
dnl   If, pursuant to or in connection with a single transaction or
dnl arrangement, you convey, or propagate by procuring conveyance of, a
dnl covered work, and grant a patent license to some of the parties
dnl receiving the covered work authorizing them to use, propagate, modify
dnl or convey a specific copy of the covered work, then the patent license
dnl you grant is automatically extended to all recipients of the covered
dnl work and works based on it.
dnl 
dnl   A patent license is "discriminatory" if it does not include within
dnl the scope of its coverage, prohibits the exercise of, or is
dnl conditioned on the non-exercise of one or more of the rights that are
dnl specifically granted under this License.  You may not convey a covered
dnl work if you are a party to an arrangement with a third party that is
dnl in the business of distributing software, under which you make payment
dnl to the third party based on the extent of your activity of conveying
dnl the work, and under which the third party grants, to any of the
dnl parties who would receive the covered work from you, a discriminatory
dnl patent license (a) in connection with copies of the covered work
dnl conveyed by you (or copies made from those copies), or (b) primarily
dnl for and in connection with specific products or compilations that
dnl contain the covered work, unless you entered into that arrangement,
dnl or that patent license was granted, prior to 28 March 2007.
dnl 
dnl   Nothing in this License shall be construed as excluding or limiting
dnl any implied license or other defenses to infringement that may
dnl otherwise be available to you under applicable patent law.
dnl 
dnl   12. No Surrender of Others' Freedom.
dnl 
dnl   If conditions are imposed on you (whether by court order, agreement or
dnl otherwise) that contradict the conditions of this License, they do not
dnl excuse you from the conditions of this License.  If you cannot convey a
dnl covered work so as to satisfy simultaneously your obligations under this
dnl License and any other pertinent obligations, then as a consequence you may
dnl not convey it at all.  For example, if you agree to terms that obligate you
dnl to collect a royalty for further conveying from those to whom you convey
dnl the Program, the only way you could satisfy both those terms and this
dnl License would be to refrain entirely from conveying the Program.
dnl 
dnl   13. Use with the GNU Affero General Public License.
dnl 
dnl   Notwithstanding any other provision of this License, you have
dnl permission to link or combine any covered work with a work licensed
dnl under version 3 of the GNU Affero General Public License into a single
dnl combined work, and to convey the resulting work.  The terms of this
dnl License will continue to apply to the part which is the covered work,
dnl but the special requirements of the GNU Affero General Public License,
dnl section 13, concerning interaction through a network will apply to the
dnl combination as such.
dnl 
dnl   14. Revised Versions of this License.
dnl 
dnl   The Free Software Foundation may publish revised and/or new versions of
dnl the GNU General Public License from time to time.  Such new versions will
dnl be similar in spirit to the present version, but may differ in detail to
dnl address new problems or concerns.
dnl 
dnl   Each version is given a distinguishing version number.  If the
dnl Program specifies that a certain numbered version of the GNU General
dnl Public License "or any later version" applies to it, you have the
dnl option of following the terms and conditions either of that numbered
dnl version or of any later version published by the Free Software
dnl Foundation.  If the Program does not specify a version number of the
dnl GNU General Public License, you may choose any version ever published
dnl by the Free Software Foundation.
dnl 
dnl   If the Program specifies that a proxy can decide which future
dnl versions of the GNU General Public License can be used, that proxy's
dnl public statement of acceptance of a version permanently authorizes you
dnl to choose that version for the Program.
dnl 
dnl   Later license versions may give you additional or different
dnl permissions.  However, no additional obligations are imposed on any
dnl author or copyright holder as a result of your choosing to follow a
dnl later version.
dnl 
dnl   15. Disclaimer of Warranty.
dnl 
dnl   THERE IS NO WARRANTY FOR THE PROGRAM, TO THE EXTENT PERMITTED BY
dnl APPLICABLE LAW.  EXCEPT WHEN OTHERWISE STATED IN WRITING THE COPYRIGHT
dnl HOLDERS AND/OR OTHER PARTIES PROVIDE THE PROGRAM "AS IS" WITHOUT WARRANTY
dnl OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO,
dnl THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
dnl PURPOSE.  THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THE PROGRAM
dnl IS WITH YOU.  SHOULD THE PROGRAM PROVE DEFECTIVE, YOU ASSUME THE COST OF
dnl ALL NECESSARY SERVICING, REPAIR OR CORRECTION.
dnl 
dnl   16. Limitation of Liability.
dnl 
dnl   IN NO EVENT UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING
dnl WILL ANY COPYRIGHT HOLDER, OR ANY OTHER PARTY WHO MODIFIES AND/OR CONVEYS
dnl THE PROGRAM AS PERMITTED ABOVE, BE LIABLE TO YOU FOR DAMAGES, INCLUDING ANY
dnl GENERAL, SPECIAL, INCIDENTAL OR CONSEQUENTIAL DAMAGES ARISING OUT OF THE
dnl USE OR INABILITY TO USE THE PROGRAM (INCLUDING BUT NOT LIMITED TO LOSS OF
dnl DATA OR DATA BEING RENDERED INACCURATE OR LOSSES SUSTAINED BY YOU OR THIRD
dnl PARTIES OR A FAILURE OF THE PROGRAM TO OPERATE WITH ANY OTHER PROGRAMS),
dnl EVEN IF SUCH HOLDER OR OTHER PARTY HAS BEEN ADVISED OF THE POSSIBILITY OF
dnl SUCH DAMAGES.
dnl 
dnl   17. Interpretation of Sections 15 and 16.
dnl 
dnl   If the disclaimer of warranty and limitation of liability provided
dnl above cannot be given local legal effect according to their terms,
dnl reviewing courts shall apply local law that most closely approximates
dnl an absolute waiver of all civil liability in connection with the
dnl Program, unless a warranty or assumption of liability accompanies a
dnl copy of the Program in return for a fee.
dnl

AC_DEFUN([AX_MPI], [
AC_PREREQ(2.50) dnl for AC_LANG_CASE

AC_LANG_CASE([C], [
    AC_REQUIRE([AC_PROG_CC])
    AC_ARG_VAR(MPICC,[MPI C compiler command])
    AC_CHECK_PROGS(MPICC, mpicc hcc mpxlc_r mpxlc mpcc cmpicc, $CC)
    acx_mpi_save_CC="$CC"
    CC="$MPICC"
    AC_SUBST(MPICC)
],
[C++], [
    AC_REQUIRE([AC_PROG_CXX])
    AC_ARG_VAR(MPICXX,[MPI C++ compiler command])
    AC_CHECK_PROGS(MPICXX, mpic++ mpicxx mpiCC hcp mpxlC_r mpxlC mpCC cmpic++, $CXX)
    acx_mpi_save_CXX="$CXX"
    CXX="$MPICXX"
    AC_SUBST(MPICXX)
],
[Fortran 77], [
    AC_REQUIRE([AC_PROG_F77])
    AC_ARG_VAR(MPIF77,[MPI Fortran 77 compiler command])
    AC_CHECK_PROGS(MPIF77, mpif77 hf77 mpxlf mpf77 mpif90 mpf90 mpxlf90 mpxlf95 mpxlf_r cmpifc cmpif90c, $F77)
    acx_mpi_save_F77="$F77"
    F77="$MPIF77"
    AC_SUBST(MPIF77)
],
[Fortran], [
    AC_REQUIRE([AC_PROG_FC])
    AC_ARG_VAR(MPIFC,[MPI Fortran compiler command])
    AC_CHECK_PROGS(MPIFC, mpif90 hf90 mpxlf90 mpxlf95 mpf90 cmpifc cmpif90c, $FC)
    acx_mpi_save_FC="$FC"
    FC="$MPIFC"
    AC_SUBST(MPIFC)
])

if test x = x"$MPILIBS"; then
    AC_LANG_CASE([C], [AC_CHECK_FUNC(MPI_Init, [MPILIBS=" "])],
        [C++], [AC_CHECK_FUNC(MPI_Init, [MPILIBS=" "])],
        [Fortran 77], [AC_MSG_CHECKING([for MPI_Init])
            AC_LINK_IFELSE([AC_LANG_PROGRAM([],[      call MPI_Init])],[MPILIBS=" "
                AC_MSG_RESULT(yes)], [AC_MSG_RESULT(no)])],
        [Fortran], [AC_MSG_CHECKING([for MPI_Init])
            AC_LINK_IFELSE([AC_LANG_PROGRAM([],[      call MPI_Init])],[MPILIBS=" "
                AC_MSG_RESULT(yes)], [AC_MSG_RESULT(no)])])
fi
AC_LANG_CASE([Fortran 77], [
    if test x = x"$MPILIBS"; then
        AC_CHECK_LIB(fmpi, MPI_Init, [MPILIBS="-lfmpi"])
    fi
    if test x = x"$MPILIBS"; then
        AC_CHECK_LIB(fmpich, MPI_Init, [MPILIBS="-lfmpich"])
    fi
],
[Fortran], [
    if test x = x"$MPILIBS"; then
        AC_CHECK_LIB(fmpi, MPI_Init, [MPILIBS="-lfmpi"])
    fi
    if test x = x"$MPILIBS"; then
        AC_CHECK_LIB(mpichf90, MPI_Init, [MPILIBS="-lmpichf90"])
    fi
])
if test x = x"$MPILIBS"; then
    AC_CHECK_LIB(mpi, MPI_Init, [MPILIBS="-lmpi"])
fi
if test x = x"$MPILIBS"; then
    AC_CHECK_LIB(mpich, MPI_Init, [MPILIBS="-lmpich"])
fi

dnl We have to use AC_TRY_COMPILE and not AC_CHECK_HEADER because the
dnl latter uses $CPP, not $CC (which may be mpicc).
AC_LANG_CASE([C], [if test x != x"$MPILIBS"; then
    AC_MSG_CHECKING([for mpi.h])
    AC_TRY_COMPILE([#include <mpi.h>],[],[AC_MSG_RESULT(yes)], [MPILIBS=""
        AC_MSG_RESULT(no)])
fi],
[C++], [if test x != x"$MPILIBS"; then
    AC_MSG_CHECKING([for mpi.h])
    AC_TRY_COMPILE([#include <mpi.h>],[],[AC_MSG_RESULT(yes)], [MPILIBS=""
        AC_MSG_RESULT(no)])
fi],
[Fortran 77], [if test x != x"$MPILIBS"; then
    AC_MSG_CHECKING([for mpif.h])
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([],[      include 'mpif.h'])],[AC_MSG_RESULT(yes)], [MPILIBS=""
        AC_MSG_RESULT(no)])
fi],
[Fortran], [if test x != x"$MPILIBS"; then
    AC_MSG_CHECKING([for mpif.h])
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([],[      include 'mpif.h'])],[AC_MSG_RESULT(yes)], [MPILIBS=""
        AC_MSG_RESULT(no)])
fi])

AC_LANG_CASE([C], [CC="$acx_mpi_save_CC"],
    [C++], [CXX="$acx_mpi_save_CXX"],
    [Fortran 77], [F77="$acx_mpi_save_F77"],
    [Fortran], [FC="$acx_mpi_save_FC"])

AC_SUBST(MPILIBS)

# Finally, execute ACTION-IF-FOUND/ACTION-IF-NOT-FOUND:
if test x = x"$MPILIBS"; then
        $2
        :
else
        ifelse([$1],,[AC_DEFINE(HAVE_MPI,1,[Define if you have the MPI library.])],[$1])
        :
fi
])dnl ACX_MPI
