def show_face_from_URL(img_url, bbox, close_after=None):
    from cufacesearch.imgio.imgio import get_buffer_from_URL
    from PIL import Image
    img_buffer = get_buffer_from_URL(img_url)
    img = Image.open(img_buffer)
    show_face(img, bbox, close_after)


def show_face(img, bbox, close_after=None):
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches

    if type(bbox) == type(dict()):
        bbox = [bbox["left"], bbox["top"], bbox["right"], bbox["bottom"]]

    # Create figure and axes
    fig, ax = plt.subplots(1)

    # Display the image
    ax.imshow(img)

    rect = patches.Rectangle((bbox[0], bbox[1]),
                               bbox[2] - bbox[0], bbox[3] - bbox[1],
                               linewidth=2, edgecolor='r', facecolor='none')

    # Add the patch to the Axes
    ax.add_patch(rect)

    if close_after:
        plt.show(block=False)
        import time
        time.sleep(close_after)
        plt.close()
    else:
        plt.show()