/* punycode.h --- Declarations for punycode functions.
   Copyright (C) 2002-2025 Simon Josefsson

   This file is part of GNU Libidn.

   GNU Libidn is free software: you can redistribute it and/or
   modify it under the terms of either:

     * the GNU Lesser General Public License as published by the Free
       Software Foundation; either version 3 of the License, or (at
       your option) any later version.

   or

     * the GNU General Public License as published by the Free
       Software Foundation; either version 2 of the License, or (at
       your option) any later version.

   or both in parallel, as here.

   GNU Libidn is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License for more details.

   You should have received copies of the GNU General Public License and
   the GNU Lesser General Public License along with this program.  If
   not, see <https://www.gnu.org/licenses/>. */

/*
 * This file is derived from RFC 3492bis written by Adam M. Costello,
 * downloaded from http://www.nicemice.net/idn/punycode-spec.gz on
 * 2015-03-02 with SHA1 a966a8017f6be579d74a50a226accc7607c40133, a
 * copy of which is stored in the GNU Libidn version controlled
 * repository under doc/specification/punycode-spec.gz.
 *
 * The changes compared to Adam's file include: re-indentation, adding
 * the license boilerplate and this comment, adding the #ifndef
 * PUNYCODE_H and IDNAPI blocks, changing the return code of
 * punycode_encode and punycode_decode from enum to int, simplifying
 * the definition of punycode_uint by #include'ing idn-int.h and using
 * uint32_t instead of limit.h-based code, adding Punycode_status and
 * punycode_strerror, adding 'extern IDNAPI' declarations to function
 * prototypes, and mentioning variable names in function prototypes.
 *
 * Adam's file contains the following:
 *
 * punycode-sample.c 2.0.0 (2004-Mar-21-Sun)
 * http://www.nicemice.net/idn/
 * Adam M. Costello
 * http://www.nicemice.net/amc/
 *
 * This is ANSI C code (C89) implementing Punycode 1.0.x.
 *
 * Disclaimer and license: Regarding this entire document or any
 * portion of it (including the pseudocode and C code), the author
 * makes no guarantees and is not responsible for any damage resulting
 * from its use.  The author grants irrevocable permission to anyone
 * to use, modify, and distribute it in any way that does not diminish
 * the rights of anyone else to use, modify, and distribute it,
 * provided that redistributed derivative works do not contain
 * misleading author or version information.  Derivative works need
 * not be licensed under similar terms.
 */

#ifndef PUNYCODE_H
# define PUNYCODE_H

/**
 * SECTION:punycode
 * @title: punycode.h
 * @short_description: Punycode-related functions
 *
 * Punycode-related functions.
 */

# ifndef IDNAPI
#   define IDNAPI
# endif

# ifdef __cplusplus
extern "C"
{
# endif

/************************************************************/
/* Public interface (would normally go in its own .h file): */

# include <stddef.h>		/* size_t */
# include <idn-int.h>		/* uint32_t */

  enum punycode_status
  {
    punycode_success = 0,
    punycode_bad_input = 1,	/* Input is invalid.                       */
    punycode_big_output = 2,	/* Output would exceed the space provided. */
    punycode_overflow = 3	/* Wider integers needed to process input. */
  };

  typedef enum
  {
    PUNYCODE_SUCCESS = punycode_success,
    PUNYCODE_BAD_INPUT = punycode_bad_input,
    PUNYCODE_BIG_OUTPUT = punycode_big_output,
    PUNYCODE_OVERFLOW = punycode_overflow
  } Punycode_status;

  extern IDNAPI const char *punycode_strerror (Punycode_status rc);

/* punycode_uint needs to be unsigned and needs to be */
/* at least 26 bits wide.  The particular type can be */
/* specified by defining PUNYCODE_UINT, otherwise a   */
/* suitable type will be chosen automatically.        */

  typedef uint32_t punycode_uint;

  extern IDNAPI int punycode_encode (size_t input_length,
				     const punycode_uint input[],
				     const unsigned char case_flags[],
				     size_t *output_length, char output[]);

/*
    punycode_encode() converts a sequence of code points (presumed to be
    Unicode code points) to Punycode.

    Input arguments (to be supplied by the caller):

        input_length
            The number of code points in the input array and the number
            of flags in the case_flags array.

        input
            An array of code points.  They are presumed to be Unicode
            code points, but that is not strictly necessary.  The
            array contains code points, not code units.  UTF-16 uses
            code units D800 through DFFF to refer to code points
            10000..10FFFF.  The code points D800..DFFF do not occur in
            any valid Unicode string.  The code points that can occur in
            Unicode strings (0..D7FF and E000..10FFFF) are also called
            Unicode scalar values.

        case_flags
            A null pointer or an array of boolean values parallel to
            the input array.  Nonzero (true, flagged) suggests that the
            corresponding Unicode character be forced to uppercase after
            being decoded (if possible), and zero (false, unflagged)
            suggests that it be forced to lowercase (if possible).
            ASCII code points (0..7F) are encoded literally, except that
            ASCII letters are forced to uppercase or lowercase according
            to the corresponding case flags.  If case_flags is a null
            pointer then ASCII letters are left as they are, and other
            code points are treated as unflagged.

    Output arguments (to be filled in by the function):

        output
            An array of ASCII code points.  It is *not* null-terminated;
            it will contain zeros if and only if the input contains
            zeros.  (Of course the caller can leave room for a
            terminator and add one if needed.)

    Input/output arguments (to be supplied by the caller and overwritten
    by the function):

        output_length
            The caller passes in the maximum number of ASCII code points
            that it can receive.  On successful return it will contain
            the number of ASCII code points actually output.

    Return value:

        Can be any of the punycode_status values defined above except
        punycode_bad_input.  If not punycode_success, then output_size
        and output might contain garbage.
*/

  extern int punycode_decode (size_t input_length,
			     const char input[],
			     size_t *output_length,
			     punycode_uint output[],
			     unsigned char case_flags[]);

/*
    punycode_decode() converts Punycode to a sequence of code points
    (presumed to be Unicode code points).

    Input arguments (to be supplied by the caller):

        input_length
            The number of ASCII code points in the input array.

        input
            An array of ASCII code points (0..7F).

    Output arguments (to be filled in by the function):

        output
            An array of code points like the input argument of
            punycode_encode() (see above).

        case_flags
            A null pointer (if the flags are not needed by the caller)
            or an array of boolean values parallel to the output array.
            Nonzero (true, flagged) suggests that the corresponding
            Unicode character be forced to uppercase by the caller (if
            possible), and zero (false, unflagged) suggests that it
            be forced to lowercase (if possible).  ASCII code points
            (0..7F) are output already in the proper case, but their
            flags will be set appropriately so that applying the flags
            would be harmless.

    Input/output arguments (to be supplied by the caller and overwritten
    by the function):

        output_length
            The caller passes in the maximum number of code points
            that it can receive into the output array (which is also
            the maximum number of flags that it can receive into the
            case_flags array, if case_flags is not a null pointer).  On
            successful return it will contain the number of code points
            actually output (which is also the number of flags actually
            output, if case_flags is not a null pointer).  The decoder
            will never need to output more code points than the number
            of ASCII code points in the input, because of the way the
            encoding is defined.  The number of code points output
            cannot exceed the maximum possible value of a punycode_uint,
            even if the supplied output_length is greater than that.

    Return value:

        Can be any of the punycode_status values defined above.  If not
        punycode_success, then output_length, output, and case_flags
        might contain garbage.
*/

# ifdef __cplusplus
}
# endif
#endif				/* PUNYCODE_H */
